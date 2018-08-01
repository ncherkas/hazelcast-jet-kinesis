package com.hazelcast.jet.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WatermarkSourceUtil;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.*;

/**
 * Amazon Kinesis Source.
 * @param <T> data type
 */
public class StreamKinesisP<T> extends AbstractProcessor {

    private static final long METADATA_CHECK_INTERVAL_NANOS = SECONDS.toNanos(10); // TODO: double check the value
//    private static final int READ_LIMIT = 10_000;
    private static final int READ_LIMIT = 1000; // For the simple test

    private final Regions region;
    private final AWSCredentials awsCredentials;
    private final String streamName;
    private final DistributedFunction<Record, T> projectionFn;
    private final WatermarkSourceUtil<T> watermarkSourceUtil;
    private final Map<String, ShardInfo> assignedShards;

    private AmazonKinesis amazonKinesis;
    private Traverser<Object> traverser = Traversers.empty(); // Why <Object> ?
    @VisibleForTesting
    Set<String> closedProcessedShards; // Hazelcast ISet
    @VisibleForTesting
    int processorIndex;
    @VisibleForTesting
    int totalParallelism;
    private long nextMetadataCheck = Long.MIN_VALUE;
    private int wsuPartitionCount;

    /**
     * TODO: multiple streams?
     * TODO: google/jet @Nonnull annotations?
     * TODO: check for retention period
     * TODO: fault tolerance
     */
    public StreamKinesisP(Regions region, AWSCredentials awsCredentials, String streamName,
                          DistributedFunction<Record, T> projectionFn,
                          WatermarkGenerationParams<? super T> wmGenParams) {

        this.region = region;
        this.awsCredentials = awsCredentials;
        this.streamName = streamName;
        this.projectionFn = projectionFn;
        this.watermarkSourceUtil = new WatermarkSourceUtil<>(wmGenParams);
        this.assignedShards = new LinkedHashMap<>();
    }

    @Override
    protected void init(Context context) {
        AWSCredentialsProvider credentialsProvider = awsCredentials != null
                ? new AWSStaticCredentialsProvider(awsCredentials)
                : new DefaultAWSCredentialsProviderChain();

        // TODO: allow to pass a client config
        this.amazonKinesis = AmazonKinesisClientBuilder.standard()
                .withRegion(region)
                .withCredentials(credentialsProvider)
                .build();

        this.processorIndex = context.globalProcessorIndex();
        this.totalParallelism = context.totalParallelism();
        this.wsuPartitionCount = 0;

        // TODO: Maybe we can inject the ISet somehow else?
        JetInstance jetClient = Jet.newJetClient();
        this.closedProcessedShards = jetClient.getHazelcastInstance().getSet("streamKinesis_" + streamName + "_closedProcessedShards");

        assignShards(false);

        getLogger().info("[#" + processorIndex + "] StreamKinesisP::init completed!");
    }


    private void assignShards(boolean checkMetadataInterval) {
        if (checkMetadataInterval && System.nanoTime() < nextMetadataCheck) {
            return;
        }

        List<Shard> shards = new ArrayList<>();

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        String exclusiveStartShardId = null;
        do {
            describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
            DescribeStreamResult describeStreamResult = amazonKinesis.describeStream(describeStreamRequest);
            shards.addAll(describeStreamResult.getStreamDescription().getShards());
            exclusiveStartShardId = describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0
                    ? shards.get(shards.size() - 1).getShardId()
                    : null;
        } while (exclusiveStartShardId != null);

        getLogger().info("[#" + processorIndex + "] All available shards: ");
        shards.forEach(s -> System.out.print(s.getShardId() + "::" + s.getParentShardId() + ", "
                + (s.getSequenceNumberRange().getEndingSequenceNumber() == null ? "open; " : "closed; ")));

        Map<String, Shard> newAssignment = getAssignment(shards);
        assignedShards.keySet().forEach(newAssignment::remove);
        if (!newAssignment.isEmpty()) {
            getLogger().info("[#" + processorIndex + "] Shard assignment has changed, added shards: " + newAssignment.keySet());

            for (Map.Entry<String, Shard> shardEntry : newAssignment.entrySet()) {
                String shardId = shardEntry.getKey();
                Shard shard = shardEntry.getValue();
                assignedShards.put(shardId, new ShardInfo(shard.getParentShardId(), shard.getAdjacentParentShardId(), assignedShards.size()));
            }

            getLogger().info("[#" + processorIndex + "] Current shard assignment: " + assignedShards.keySet());

            // TODO: Hack, double check this
            int assignedShardsCount = assignedShards.size();
            if (assignedShardsCount > wsuPartitionCount) {
                getLogger().info("[#" + processorIndex + "] Hacking the Watermark Source Util...");
                wsuPartitionCount = assignedShardsCount;
                watermarkSourceUtil.increasePartitionCount(assignedShardsCount);
            }
        }

        nextMetadataCheck = System.nanoTime() + METADATA_CHECK_INTERVAL_NANOS;
    }

    @VisibleForTesting
    Map<String, Shard> getAssignment(List<Shard> shards) {
        Map<String, Shard> shardsById = shards.stream()
                .collect(toMap(Shard::getShardId, Function.identity()));

        Map<String, List<Shard>> shardsByParentId = shards.stream()
                .filter(s -> shardsById.containsKey(s.getParentShardId()))
                .collect(Collectors.groupingBy(Shard::getParentShardId));

        Map<String, List<Shard>> shardsByAdjacentParentId = shards.stream()
                .filter(s -> shardsById.containsKey(s.getAdjacentParentShardId()))
                .collect(Collectors.groupingBy(Shard::getAdjacentParentShardId));

        com.google.common.graph.Traverser<Shard> shardsTraverser = com.google.common.graph.Traverser.forGraph(s ->
                shardsByParentId.getOrDefault(s.getShardId(),
                        shardsByAdjacentParentId.getOrDefault(s.getShardId(),
                                emptyList())));

        List<Shard> rootShards = shardsById.values().stream()
                .filter(s -> !shardsById.containsKey(s.getParentShardId()))
                .collect(toList());

        Iterable<Shard> bfTraversedShards = shardsTraverser.breadthFirst(rootShards);

        getLogger().info("[#" + processorIndex + "] BF Traversal: " + Iterables.transform(bfTraversedShards, Shard::getShardId));
        List<Shard> bfOrderedShards = Lists.newArrayList(bfTraversedShards);

        Map<String, Shard> assignment = new LinkedHashMap<>();
        for (int i = 0; i < bfOrderedShards.size(); i++) {
            Shard shard = bfOrderedShards.get(i);
            if (i % totalParallelism == processorIndex) {
                assignment.put(shard.getShardId(), shard);
            }
        }
        return assignment;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        assignShards(true);

        List<Traverser<Object>> partialResultTraversers = new ArrayList<>();
        Set<String> closedShards = new HashSet<>();

        // TODO: check if consumer.poll(POLL_TIMEOUT_MS); in Kafka shuffles the messages
        for (Map.Entry<String, ShardInfo> assignedShardEntry : assignedShards.entrySet()) {
            String shardId = assignedShardEntry.getKey();
            ShardInfo shardInfo = assignedShardEntry.getValue();
            String parentShardId = shardInfo.getParentShardId();
            String adjacentParentShardId = shardInfo.getAdjacentParentShardId();
            if (closedProcessedShards.contains(shardId)
                    || (parentShardId != null && !closedProcessedShards.contains(parentShardId))
                    || (adjacentParentShardId != null && !closedProcessedShards.contains(adjacentParentShardId))) {

//                getLogger().info("[#" + processorIndex + "] Won't read the shard " + shardInfo + "...");

                continue;
            }

            getLogger().info("[#" + processorIndex + "] Reading the data from shard " + shardId + "...");

            String shardIterator = shardInfo.getCurrentShardIterator();
            String lastSequenceNumber = shardInfo.getLastSequenceNumber();
            int wmSourceUtilIndex = shardInfo.getWmSourceUtilIndex();

            if (shardIterator == null) {
                getLogger().info("[#" + processorIndex + "] Retrieving the shard iterator...");

                GetShardIteratorRequest shardIteratorRequest = new GetShardIteratorRequest()
                        .withStreamName(streamName)
                        .withShardId(shardId)
                        .withShardIteratorType(lastSequenceNumber != null ? AFTER_SEQUENCE_NUMBER : TRIM_HORIZON);

                if (lastSequenceNumber != null) {
                    shardIteratorRequest.setStartingSequenceNumber(lastSequenceNumber);
                }

                GetShardIteratorResult shardIteratorResult = amazonKinesis.getShardIterator(shardIteratorRequest);
                shardIterator = shardIteratorResult.getShardIterator();
            }

            try {
                GetRecordsRequest getRecordsRequest = new GetRecordsRequest()
                        .withShardIterator(shardIterator)
                        .withLimit(READ_LIMIT);
                GetRecordsResult getRecordsResult = amazonKinesis.getRecords(getRecordsRequest);

                List<Record> shardRecords = getRecordsResult.getRecords(); // TODO: handle expired iterator and other stuff

                Traverser<Object> shardRecordsTraverser = shardRecords.isEmpty()
                        ? watermarkSourceUtil.handleNoEvent()
                        : traverseIterable(shardRecords).flatMap(record -> {
                            shardInfo.setLastSequenceNumber(record.getSequenceNumber());
                            T projectedRecord = projectionFn.apply(record);
                            if (projectedRecord == null) {
                                return Traversers.empty();
                            }
                            return watermarkSourceUtil.handleEvent(projectedRecord, wmSourceUtilIndex);
                        });

                partialResultTraversers.add(shardRecordsTraverser);

                String nextShardIterator = getRecordsResult.getNextShardIterator();
                if (nextShardIterator == null) {
                    // Shard has been closed and we've processed all the data in it
                    closedShards.add(shardId);
                }

                shardInfo.setCurrentShardIterator(nextShardIterator);
            } catch (ExpiredIteratorException e) {
                // For now let's assume that exceptions and other faults of the processor are retried by the Jet
                shardInfo.setCurrentShardIterator(null);
                throw e;
            }
        }

        if (!closedShards.isEmpty()) {
            getLogger().info("[#" + processorIndex + "] The following closed shards has been processed: " + closedShards);
            closedProcessedShards.addAll(closedShards);

            getLogger().info("[#" + processorIndex + "] All closed processed shards: ");
            closedProcessedShards.forEach(System.out::println);

            assignShards(false);
        }

        if (partialResultTraversers.isEmpty()) {
            Utils.sleepInterruptibly(10, TimeUnit.MILLISECONDS); // TODO: double check this
        }

        traverser = traverseIterable(partialResultTraversers).flatMap(Function.identity());

        emitFromTraverser(traverser);

        return false;
    }

    @Override
    public void close(Throwable error) {
        getLogger().info("[#" + processorIndex + "] Releasing the resources...");
        if (amazonKinesis != null) {
            amazonKinesis.shutdown();
        }
    }

    public static <T> DistributedSupplier<Processor> processorSupplier(
            Regions region, AWSCredentials awsCredentials, String streamName,
            DistributedFunction<Record, T> projectionFn,
            WatermarkGenerationParams<? super T> wmGenParams
    ) {
        return () -> new StreamKinesisP<>(region, awsCredentials, streamName, projectionFn, wmGenParams);
    }


    public static class ShardInfo {

        private String parentShardId;
        private String adjacentParentShardId;
        private String currentShardIterator;
        private String lastSequenceNumber;
        private int wmSourceUtilIndex;

        public ShardInfo(String parentShardId, String adjacentParentShardId, int wmSourceUtilIndex) {
            this.parentShardId = parentShardId;
            this.adjacentParentShardId = adjacentParentShardId;
            this.wmSourceUtilIndex = wmSourceUtilIndex;
        }

        public String getParentShardId() {
            return parentShardId;
        }

        public void setParentShardId(String parentShardId) {
            this.parentShardId = parentShardId;
        }

        public String getAdjacentParentShardId() {
            return adjacentParentShardId;
        }

        public void setAdjacentParentShardId(String adjacentParentShardId) {
            this.adjacentParentShardId = adjacentParentShardId;
        }

        public String getCurrentShardIterator() {
            return currentShardIterator;
        }

        public void setCurrentShardIterator(String currentShardIterator) {
            this.currentShardIterator = currentShardIterator;
        }

        public String getLastSequenceNumber() {
            return lastSequenceNumber;
        }

        public void setLastSequenceNumber(String lastSequenceNumber) {
            this.lastSequenceNumber = lastSequenceNumber;
        }

        public int getWmSourceUtilIndex() {
            return wmSourceUtilIndex;
        }

        public void setWmSourceUtilIndex(int wmSourceUtilIndex) {
            this.wmSourceUtilIndex = wmSourceUtilIndex;
        }

        @Override
        public String toString() {
            return "ShardInfo{" +
                    "parentShardId='" + parentShardId + '\'' +
                    ", adjacentParentShardId='" + adjacentParentShardId + '\'' +
                    ", currentShardIterator='" + currentShardIterator + '\'' +
                    ", lastSequenceNumber='" + lastSequenceNumber + '\'' +
                    ", wmSourceUtilIndex=" + wmSourceUtilIndex +
                    '}';
        }
    }
}

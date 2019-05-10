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
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WatermarkSourceUtil;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.logging.ILogger;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.kinesis.Utils.sleepInterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Amazon Kinesis Source.
 * @param <T> data type
 */
public class StreamKinesisP<T> extends AbstractProcessor {

    private static final long METADATA_CHECK_INTERVAL_NANOS = SECONDS.toNanos(5);

    private final Regions region;
    private final AWSCredentials awsCredentials;
    private final String streamName;
    private final DistributedFunction<Record, T> projectionFn;
    private final WatermarkSourceUtil<T> watermarkSourceUtil;
    private final Map<String, ShardInfo> assignedShards;

    private KinesisClient kinesisClient;
    private Traverser<Object> traverser = Traversers.empty(); // Why <Object> ?
    @VisibleForTesting
    Set<String> closedProcessedShards; // Hazelcast ISet
    @VisibleForTesting
    int processorIndex;
    @VisibleForTesting
    int totalParallelism;
    private long nextMetadataCheck = Long.MIN_VALUE;
    private int wsuPartitionCount;

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

        AmazonKinesis amazonKinesis = AmazonKinesisClientBuilder.standard()
                .withRegion(region)
                .withCredentials(credentialsProvider)
                .build();

        this.kinesisClient = new KinesisClient(streamName, amazonKinesis);

        this.processorIndex = context.globalProcessorIndex();
        this.totalParallelism = context.totalParallelism();
        this.wsuPartitionCount = 0;

        JetInstance jetInstance = context.jetInstance();
        this.closedProcessedShards =
                jetInstance.getHazelcastInstance().getSet("streamKinesis_" + streamName + "_closedProcessedShards");

        assignShards(false);

        logger().info("[#" + processorIndex + "] StreamKinesisP::init completed!");
    }

    @VisibleForTesting
    ILogger logger() {
        return getLogger();
    }

    private void assignShards(boolean checkMetadataInterval) {
        if (checkMetadataInterval && System.nanoTime() < nextMetadataCheck) {
            return;
        }

        List<Shard> shards = kinesisClient.getShards();
        logAllShards(shards);

        Map<String, Shard> newAssignment = getAssignment(shards);
        assignedShards.keySet().forEach(newAssignment::remove);
        if (!newAssignment.isEmpty()) {
            logger().info("[#" + processorIndex + "] Shard assignment has changed, added shards: "
                    + newAssignment.keySet());

            for (Map.Entry<String, Shard> shardEntry : newAssignment.entrySet()) {
                String shardId = shardEntry.getKey();
                Shard shard = shardEntry.getValue();
                assignedShards.put(shardId, new ShardInfo(shard.getParentShardId(),
                        shard.getAdjacentParentShardId(), assignedShards.size())); // TODO: check if it breaks anything if wm util index > shards count
            }

            logger().info("[#" + processorIndex + "] Current shard assignment: " + assignedShards.keySet());

            int assignedShardsCount = assignedShards.size();
            if (assignedShardsCount > wsuPartitionCount) {
                wsuPartitionCount = assignedShardsCount;
                watermarkSourceUtil.increasePartitionCount(assignedShardsCount);
            } else {
                logger().info("[#" + processorIndex + "] Hacking the Watermark Source Util...");
            }
        }

        nextMetadataCheck = System.nanoTime() + METADATA_CHECK_INTERVAL_NANOS;
    }

    private void logAllShards(List<Shard> shards) {
        StringBuffer allShardsMsgBuilder = new StringBuffer("[#" + processorIndex + "] All available shards: ");
        shards.forEach(s -> allShardsMsgBuilder.append(s.getShardId()).append("::")
                .append(s.getParentShardId()).append(", ")
                .append(s.getAdjacentParentShardId()).append(", ")
                .append(s.getSequenceNumberRange().getEndingSequenceNumber() == null ? "open; " : "closed; "));
        logger().info(allShardsMsgBuilder.toString());
    }

    @VisibleForTesting
    Map<String, Shard> getAssignment(List<Shard> shards) {
        // Usually the response comes sorted but let's make sure we deal with the same order every time
        // ...and yes, now we really depend on the shard ids
        shards.sort(Comparator.comparing(Shard::getShardId));

        // TODO: do a thorough testing of a new simplified approach and think about plan "B" (when id format changes)
        Map<String, Shard> assignment = new LinkedHashMap<>();
        for (int i = 0; i < shards.size(); i++) {
            Shard shard = shards.get(i);
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

        // TODO: check if consumer.poll(POLL_TIMEOUT_MS); in Kafka shuffles/sorts the messages
        for (Map.Entry<String, ShardInfo> assignedShardEntry : assignedShards.entrySet()) {
            String shardId = assignedShardEntry.getKey();
            ShardInfo shardInfo = assignedShardEntry.getValue();
            String parentShardId = shardInfo.getParentShardId();
            String adjacentParentShardId = shardInfo.getAdjacentParentShardId();
            // So, imagine that re-assignment happens we check if this shard has been already processed,
            // or if it wasn't but its parent is still being processed
            // If any of these conditions is TRUE we skip this shard
            if (closedProcessedShards.contains(shardId)
                    || (parentShardId != null && !closedProcessedShards.contains(parentShardId))
                    || (adjacentParentShardId != null && !closedProcessedShards.contains(adjacentParentShardId))) {

                continue;
            }

            logger().info("[#" + processorIndex + "] Reading the data from shard " + shardId + "...");

            String shardIterator = shardInfo.getCurrentShardIterator();
            String lastSequenceNumber = shardInfo.getLastSequenceNumber();
            int wmSourceUtilIndex = shardInfo.getWmSourceUtilIndex();

            if (shardIterator == null) {
                logger().info("[#" + processorIndex + "] Retrieving the shard iterator...");
                shardIterator = kinesisClient.getShardIterator(shardId, lastSequenceNumber);
            }

            try {
                // TODO: handle expired iterator
                GetRecordsResult getRecordsResult = kinesisClient.getRecords(shardIterator);
                List<Record> shardRecords = getRecordsResult.getRecords();

                Traverser<Object> shardRecordsTraverser = shardRecords.isEmpty()
                        ? watermarkSourceUtil.handleNoEvent()
                        : traverseIterable(shardRecords).flatMap(record -> {
                            shardInfo.setLastSequenceNumber(record.getSequenceNumber());
                            T projectedRecord = projectionFn.apply(record);
                            if (projectedRecord == null) { // TODO: why? because Jet doesn't work with nulls?
                                return Traversers.empty();
                            }
                            return watermarkSourceUtil.handleEvent(projectedRecord, wmSourceUtilIndex);
                        });

                partialResultTraversers.add(shardRecordsTraverser);

                String nextShardIterator = getRecordsResult.getNextShardIterator();
                if (nextShardIterator == null) {
                    // Shard has been closed and we've processed all its data
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
            logger().info("[#" + processorIndex + "] The following closed shards has been processed: " + closedShards);
            closedProcessedShards.addAll(closedShards);

            logAllClosedShards(closedProcessedShards);

            assignShards(false);
        }

        if (partialResultTraversers.isEmpty()) {
            sleepInterruptibly(10, TimeUnit.MILLISECONDS);
        }

        traverser = traverseIterable(partialResultTraversers).flatMap(Function.identity());
        emitFromTraverser(traverser);

        return false;
    }

    private void logAllClosedShards(Set<String> closedShards) {
        StringBuffer allClosedShardsMsgBuilder = new StringBuffer("[#" + processorIndex + "] All closed processed shards: ");
        closedShards.forEach(s -> allClosedShardsMsgBuilder.append(s).append(", "));
        logger().info(allClosedShardsMsgBuilder.toString());
    }

    @Override
    public void close() {
        logger().info("[#" + processorIndex + "] Releasing the resources...");
        if (kinesisClient != null) {
            kinesisClient.close();
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

        private final String parentShardId;
        private final String adjacentParentShardId;
        private final int wmSourceUtilIndex;

        private String currentShardIterator;
        private String lastSequenceNumber;

        public ShardInfo(String parentShardId, String adjacentParentShardId, int wmSourceUtilIndex) {
            this.parentShardId = parentShardId;
            this.adjacentParentShardId = adjacentParentShardId;
            this.wmSourceUtilIndex = wmSourceUtilIndex;
        }

        public String getParentShardId() {
            return parentShardId;
        }

        public String getAdjacentParentShardId() {
            return adjacentParentShardId;
        }

        public int getWmSourceUtilIndex() {
            return wmSourceUtilIndex;
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

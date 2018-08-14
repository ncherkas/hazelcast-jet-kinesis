package com.hazelcast.jet.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Amazon Kinesis Sink.
 * @param <T> data type
 */
public class WriteKinesisP<T> implements Processor {

    private static final int PUT_RECORDS_LIMIT = 500;

    private final KinesisClient kinesisClient;
    private final DistributedFunction<T, String> partitionKeyFn;
    private final DistributedFunction<T, ByteBuffer> toByteBufferFn;
    private final Queue<T> buffer = new ArrayDeque<>(PUT_RECORDS_LIMIT);
    private final AtomicLong putsCounter = new AtomicLong(); // For test purpose

    private int processorIndex;
    private ILogger logger;

    public WriteKinesisP(KinesisClient kinesisClient, DistributedFunction<T, String> partitionKeyFn, DistributedFunction<T, ByteBuffer> toByteBufferFn) {
        this.kinesisClient = kinesisClient;
        this.partitionKeyFn = partitionKeyFn;
        this.toByteBufferFn = toByteBufferFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        this.processorIndex = context.globalProcessorIndex();
        this.logger = context.logger();
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(int ordinal, @Nonnull Inbox inbox) {
        while (!inbox.isEmpty()) {
            buffer.add((T) inbox.poll());
            if (buffer.size() == PUT_RECORDS_LIMIT) {
                flush();
            }
        }

        // TODO: Do we need this if we've already implemented complete() ?
        flush();
    }

    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }

        List<PutRecordsRequestEntry> records = buffer.stream()
                .map(this::toPutRecordsRequestEntry)
                .collect(toList());

        kinesisClient.putRecords(records);
        logger.info("[#" + processorIndex + "] " + putsCounter.incrementAndGet() + " batch puts so far...");

        buffer.clear();
    }

    private PutRecordsRequestEntry toPutRecordsRequestEntry(T item) {
        return new PutRecordsRequestEntry()
                .withPartitionKey(partitionKeyFn.apply(item))
                .withData(toByteBufferFn.apply(item));
    }

    @Override
    public boolean complete() {
        flush();
        return true;
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {

    }

    public static class Supplier<T> implements ProcessorSupplier {

        private static final long serialVersionUID = 1L;

        private final String streamName;
        private final Regions region;
        private final AWSCredentials awsCredentials;
        private final DistributedFunction<T, String> partitionKeyFn;
        private final DistributedFunction<T, ByteBuffer> toByteBufferFn;

        private transient KinesisClient kinesisClient;

        public Supplier(Regions region, AWSCredentials awsCredentials, String streamName,
                        DistributedFunction<T, String> partitionKeyFn,
                        DistributedFunction<T, ByteBuffer> toByteBufferFn) {

            this.streamName = streamName;
            this.region = region;
            this.awsCredentials = awsCredentials;
            this.partitionKeyFn = partitionKeyFn;
            this.toByteBufferFn = toByteBufferFn;
        }

        @Override
        public void init(@Nonnull Context context) {
            AWSCredentialsProvider credentialsProvider = awsCredentials != null
                    ? new AWSStaticCredentialsProvider(awsCredentials)
                    : new DefaultAWSCredentialsProviderChain();

            AmazonKinesis amazonKinesis = AmazonKinesisClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(credentialsProvider)
                    .build();

            this.kinesisClient = new KinesisClient(streamName, amazonKinesis);
        }

        @Override
        public List<Processor> get(int count) {
            return Stream.generate(() -> new WriteKinesisP<>(kinesisClient, partitionKeyFn, toByteBufferFn))
                    .limit(count)
                    .collect(toList());
        }

        @Override
        public void close(Throwable error) {
            if (kinesisClient != null) {
                kinesisClient.close();
            }
        }
    }
}

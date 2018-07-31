package com.hazelcast.jet.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class WriteKinesisP<T> implements Processor {

    private final String streamName;
    private final AmazonKinesis amazonKinesis;
    private final DistributedFunction<T, String> partitionKeyFn;
    private final DistributedFunction<T, ByteBuffer> toByteBufferFn;

    // For test purpose
    private final AtomicLong putsCounter = new AtomicLong();
    private int processorIndex;

    /**
     * TODO: implement batching
     */
    public WriteKinesisP(String streamName, AmazonKinesis amazonKinesis, DistributedFunction<T, String> partitionKeyFn, DistributedFunction<T, ByteBuffer> toByteBufferFn) {
        this.streamName = streamName;
        this.amazonKinesis = amazonKinesis;
        this.partitionKeyFn = partitionKeyFn;
        this.toByteBufferFn = toByteBufferFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        this.processorIndex = context.globalProcessorIndex();
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain((Object item) -> {
            PutRecordRequest putRecordRequest = new PutRecordRequest()
                    .withStreamName(streamName)
                    .withPartitionKey(partitionKeyFn.apply((T) item))
                    .withData(toByteBufferFn.apply((T) item));

            amazonKinesis.putRecord(putRecordRequest);

            if (putsCounter.incrementAndGet() % 100 == 0) {
                System.out.println("[#" + processorIndex + "] " + putsCounter.get() + " puts so far...");
            }

        });
    }

    @Override
    public boolean complete() {
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

        private transient AmazonKinesis amazonKinesis;

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

            // TODO: allow to pass a client config
            this.amazonKinesis = AmazonKinesisClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(credentialsProvider)
                    .build();
        }

        @Override
        public List<Processor> get(int count) {
            return Stream.generate(() -> new WriteKinesisP<>(streamName, amazonKinesis, partitionKeyFn, toByteBufferFn))
                    .limit(count)
                    .collect(toList());
        }

        @Override
        public void close(Throwable error) {
            if (amazonKinesis != null) {
                amazonKinesis.shutdown();
            }
        }
    }
}

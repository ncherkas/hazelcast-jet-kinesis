package com.hazelcast.jet.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON;

/**
 * Works with Amazon Kinesis API.
 * TODO: switch to AWS Async SDK v2 since the Kinesis part of it is already recommended for prod use
 */
public class KinesisClient implements Closeable {

//    private static final int READ_LIMIT = 10_000;
    private static final int READ_LIMIT = 1000; // For the simple test

    private final String streamName;
    private final AmazonKinesis amazonKinesis;

    public KinesisClient(String streamName, AmazonKinesis amazonKinesis) {
        this.streamName = streamName;
        this.amazonKinesis = amazonKinesis;
    }

    public List<Shard> getShards() {
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

        return shards;
    }

    public String getShardIterator(String shardId, String lastSequenceNumber) {
        GetShardIteratorRequest shardIteratorRequest = new GetShardIteratorRequest()
                .withStreamName(streamName)
                .withShardId(shardId)
                .withShardIteratorType(lastSequenceNumber != null ? AFTER_SEQUENCE_NUMBER : TRIM_HORIZON);

        if (lastSequenceNumber != null) {
            shardIteratorRequest.setStartingSequenceNumber(lastSequenceNumber);
        }

        GetShardIteratorResult shardIteratorResult = amazonKinesis.getShardIterator(shardIteratorRequest);
        return shardIteratorResult.getShardIterator();
    }

    public GetRecordsResult getRecords(String shardIterator) {
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest()
                .withShardIterator(shardIterator)
                .withLimit(READ_LIMIT);
        return amazonKinesis.getRecords(getRecordsRequest);
    }

    @Override
    public void close() {
        amazonKinesis.shutdown();
    }
}

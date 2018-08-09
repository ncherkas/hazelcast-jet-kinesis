package com.hazelcast.jet.kinesis.sample;

import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.stream.Collectors.toList;

/**
 * Just for test purpose. Will be removed later.
 */
public class KinesisTestClient {

    /**
     * TODO: consider https://aws.amazon.com/blogs/big-data/test-your-streaming-data-solution-with-the-new-amazon-kinesis-data-generator/
     */

    private static final String DEFAULT_STREAM_NAME = "nc_test_stream_01";
    private static final Regions DEFAULT_REGION = Regions.EU_CENTRAL_1;
    private static final int WRITE_LIMIT = 100_000;
    private static final int READ_LIMIT = 10_000;

    private final String streamName;
    private final AmazonKinesis amazonKinesis;
    private final ObjectMapper objectMapper;

    public KinesisTestClient(String streamName, Regions region, String accessKey, String secretKey) {
        this.streamName = streamName;

        List<AWSCredentialsProvider> chain = new ArrayList<>();
        if (!Strings.isNullOrEmpty(accessKey) && !Strings.isNullOrEmpty(secretKey)) {
            chain.add(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        }

        chain.add(new EnvironmentVariableCredentialsProvider());
        chain.add(new SystemPropertiesCredentialsProvider());
        chain.add(new ProfileCredentialsProvider());
        chain.add(new EC2ContainerCredentialsProviderWrapper());

        this.amazonKinesis = AmazonKinesisClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSCredentialsProviderChain(chain))
                .build();

        this.objectMapper = new ObjectMapper();
    }

    public void push(String userKey, TestEvent.Type type, Map<String, Object> data) {
        TestEvent event =
                new TestEvent(UUID.randomUUID().toString(), userKey, type, data, Instant.now().toEpochMilli());

        PutRecordRequest putRecordRequest;
        try {
            putRecordRequest = new PutRecordRequest()
                    .withPartitionKey(userKey)
                    .withStreamName(streamName)
                    .withData(ByteBuffer.wrap(objectMapper.writeValueAsBytes(event)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize the test event instance " + event, e);
        }

        amazonKinesis.putRecord(putRecordRequest);
    }

    public List<TestEvent> read() {
        DescribeStreamResult describeStreamResult = amazonKinesis.describeStream(DEFAULT_STREAM_NAME);
        // System.out.println("Stream Describe Result: " + describeStreamResult);

        List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

        Preconditions.checkState(shards.size() == 1, "Stream count > 1 not supported yet");

        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                .withStreamName(streamName)
                .withShardId(shards.get(0).getShardId())
                .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);

        GetShardIteratorResult shardIteratorResult = amazonKinesis.getShardIterator(getShardIteratorRequest);

        String shardIterator = shardIteratorResult.getShardIterator();

        GetRecordsRequest getRecordsRequest = new GetRecordsRequest()
                .withLimit(READ_LIMIT)
                .withShardIterator(shardIterator);

        GetRecordsResult recordsResult = amazonKinesis.getRecords(getRecordsRequest);

        return recordsResult.getRecords().stream()
                .map(this::toTestEvent)
                .collect(toList());
    }

    private TestEvent toTestEvent(Record record) {
        try {
            return objectMapper.readValue(record.getData().array(), TestEvent.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize the test event instance", e);
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("You must provide one of the commands: read, write");
            System.exit(0);
        }

        KinesisTestClient testClient = new KinesisTestClient(DEFAULT_STREAM_NAME, DEFAULT_REGION, null, null);
        for (int i = 0; i < args.length; i++) {
            String command = args[i];
            switch (command) {
                case "describe":
                    testClient.describe();
                    break;
                case "write":
                    write(testClient);
                    break;
                case "read":
                    read(testClient);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported command: " + command);
            }
        }
    }

    private void describe() {
        DescribeStreamResult describeStreamResult = amazonKinesis.describeStream(DEFAULT_STREAM_NAME);
        List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
        System.out.println(shards);
    }

    private static void write(KinesisTestClient testClient) {
        System.out.println("Writing " + WRITE_LIMIT + " events to the stream...");

        for (int i = 0; i < WRITE_LIMIT; i++) {
            String userKey = "u" + ThreadLocalRandom.current().nextInt(1, 11);
            String device = ThreadLocalRandom.current().nextBoolean() ? "laptop" : "mobile";
            TestEvent.Type type = TestEvent.Type.values()[ThreadLocalRandom.current().nextInt(0, TestEvent.Type.values().length)];
            Map<String, Object> data = Maps.newHashMap(Collections.singletonMap("device", device));
            testClient.push(userKey, type, data);

            if ((i + 1) % 1000 == 0) {
                System.out.println((i + 1) + " events written so far...");
            }
        }
    }

    private static void read(KinesisTestClient testClient) {
        List<TestEvent> events = testClient.read();
        System.out.println("--------------------- Events in the stream (only first 100 of them) ---------------------");
        events.stream()
                .limit(100)
                .forEach(System.out::println);
        System.out.println("...");
    }

    public static class TestEvent {

        public enum Type { LOGIN, PRODUCT_CLICK, ADDED_TO_BASKED, ORDERED, LOGOUT }

        private final String id;
        private final String userKey;
        private final Type type;
        private final Map<String, Object> data;
        private final long timestampMillis;

        @JsonCreator
        public TestEvent(@JsonProperty("id") String id, @JsonProperty("userKey") String userKey, @JsonProperty("type") Type type,
                         @JsonProperty("data") Map<String, Object> data, @JsonProperty("timestampMillis") long timestampMillis) {

            this.id = id;
            this.userKey = userKey;
            this.type = type;
            this.data = data;
            this.timestampMillis = timestampMillis;
        }

        public String getId() {
            return id;
        }

        public String getUserKey() {
            return userKey;
        }

        public Type getType() {
            return type;
        }

        public Map<String, Object> getData() {
            return data;
        }

        /**
         * TODO: try {@code Instant} ?
         * @return timestampMillis
         */
        public long getTimestampMillis() {
            return timestampMillis;
        }

        @Override
        public String toString() {
            return "TestEvent{" +
                    "id='" + id + '\'' +
                    ", userKey='" + userKey + '\'' +
                    ", type=" + type +
                    ", data=" + data +
                    ", timestampMillis=" + timestampMillis +
                    '}';
        }
    }
}

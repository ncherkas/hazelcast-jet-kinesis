package com.hazelcast.jet.kinesis.sample;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.kinesis.StreamKinesisP;
import com.hazelcast.jet.kinesis.WriteKinesisP;
import com.hazelcast.jet.pipeline.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.amazonaws.regions.Regions.EU_CENTRAL_1;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.Sources.streamFromProcessorWithWatermarks;

public class TestApp {

    private static final int PREFERRED_LOCAL_PARALLELISM = 2;

    private static final String SOURCE_STREAM = "nc_test_stream_03";
    private static final String SINK_STREAM = "nc_test_stream_04";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        runKinesisTest();
    }

    private static void runKinesisTest() {
        Pipeline pipeline = Pipeline.create();

        StreamStage<TimestampedEntry<String, Long>> streamingStage = pipeline.<KinesisTestClient.TestEvent>drawFrom(streamFromProcessorWithWatermarks("streamKinesis",
                    w -> streamKinesisP(EU_CENTRAL_1, null, SOURCE_STREAM, TestApp::toTestEvent, w)))
                .addTimestamps(KinesisTestClient.TestEvent::getTimestampMillis, 1000)
                .window(WindowDefinition.tumbling(10_000))
                .groupingKey(KinesisTestClient.TestEvent::getUserKey)
                .aggregate(counting());

        streamingStage.map(OBJECT_MAPPER::writeValueAsString).drainTo(Sinks.logger());

        streamingStage.drainTo(Sinks.fromProcessor("writeKinesis",
                writeKinesisP(EU_CENTRAL_1, null, SINK_STREAM, TimestampedEntry::getKey, TestApp::toByteBuffer)));

        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance(); // 2 members running locally

        Job job = jet.newJob(pipeline);
        System.out.println("--- Job started ---");
        job.join();

        Jet.shutdownAll();
    }

    public static ByteBuffer toByteBuffer(TimestampedEntry<String, Long> te) throws Exception {
        return ByteBuffer.wrap(OBJECT_MAPPER.writeValueAsBytes(te));
    }

    public static KinesisTestClient.TestEvent toTestEvent(Record record) {
        try {
            // TODO: don't use a global {@code OBJECT_MAPPER}
            return OBJECT_MAPPER.readValue(record.getData().array(), KinesisTestClient.TestEvent.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize the test event instance", e);
        }
    }

    public static <T> ProcessorMetaSupplier streamKinesisP(
            Regions region, AWSCredentials awsCredentials, String streamName,
            DistributedFunction<Record, T> projectionFn,
            WatermarkGenerationParams<? super T> wmGenParams
    ) {
        return ProcessorMetaSupplier.of(
                StreamKinesisP.processorSupplier(region, awsCredentials, streamName, projectionFn, wmGenParams),
                PREFERRED_LOCAL_PARALLELISM
        );
    }

    public static <T> ProcessorMetaSupplier writeKinesisP(
            Regions region, AWSCredentials awsCredentials, String streamName,
            DistributedFunction<T, String> partitionKeyFn,
            DistributedFunction<T, ByteBuffer> toByteBufferFn
    ) {
        return ProcessorMetaSupplier.of(new WriteKinesisP.Supplier<>(region, awsCredentials, streamName, partitionKeyFn, toByteBufferFn), PREFERRED_LOCAL_PARALLELISM);
    }

}

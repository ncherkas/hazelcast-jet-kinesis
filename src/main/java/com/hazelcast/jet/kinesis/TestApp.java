package com.hazelcast.jet.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.Sources.streamFromProcessorWithWatermarks;

public class TestApp {

    private static final int PREFERRED_LOCAL_PARALLELISM = 1;
//    private static final int PREFERRED_LOCAL_PARALLELISM = 2;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        runKinesisTest();
    }

    private static void runKinesisTest() {
        Pipeline p = Pipeline.create();

        p.<Record>drawFrom(streamFromProcessorWithWatermarks("streamKinesis", w -> streamKinesisP(Regions.EU_CENTRAL_1, null, "nc_test_stream_01", DistributedFunction.identity(), w)))
//                .addTimestamps(r -> r.getApproximateArrivalTimestamp().getTime(), 10)
                .map(TestApp::toTestEvent)
//                .window(WindowDefinition.tumbling(10_000))
//                .aggregate(counting())
//                .drainTo(Sinks.noop());
                // Can't use ObjectMapper within the Distributed Function
                .drainTo(Sinks.fromProcessor("writeKinesis", writeKinesisP(Regions.EU_CENTRAL_1, null, "nc_test_stream_02", te -> ((KinesisTestClient.TestEvent) te).getUserKey(), te -> ByteBuffer.wrap(te.toString().getBytes()))));
//                .drainTo(Sinks.logger());

        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();

        Job job = jet.newJob(p);
        System.out.println("--- Job started ---");
        job.join();

        Jet.shutdownAll();
    }

    public static KinesisTestClient.TestEvent toTestEvent(Record record) {
        try {
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

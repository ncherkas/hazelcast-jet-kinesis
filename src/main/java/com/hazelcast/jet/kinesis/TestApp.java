package com.hazelcast.jet.kinesis;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.kinesis.numbers.GenerateNumbersPMetaSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;

public class TestApp {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        StreamStage<Integer> stage = p.drawFrom(Sources.streamFromProcessor("numbers", new GenerateNumbersPMetaSupplier(100)));

        stage.map(i -> "Number #" + i).drainTo(Sinks.logger());

        JetConfig config = new JetConfig();
//        config.getInstanceConfig().setCooperativeThreadCount(4);
        JetInstance jet = Jet.newJetInstance(config);
        Jet.newJetInstance(config);

        Job job = jet.newJob(p);
        System.out.println("--- Job started ---");
        job.join();

        Jet.shutdownAll();
    }
}

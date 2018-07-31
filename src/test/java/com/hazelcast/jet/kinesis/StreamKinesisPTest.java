package com.hazelcast.jet.kinesis;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.function.DistributedFunction;
import org.testng.annotations.Test;

import java.util.*;

public class StreamKinesisPTest {

    @Test
    public void testAssignShards() {
        StreamKinesisP<Record> kinesisP =
                new StreamKinesisP<>(Regions.DEFAULT_REGION, null, "s1", DistributedFunction.identity(), WatermarkGenerationParams.noWatermarks());

        kinesisP.totalParallelism = 4;
        kinesisP.closedProcessedShards = new HashSet<>();
        kinesisP.closedProcessedShards.add("shardId-000000000000");
        kinesisP.closedProcessedShards.add("shardId-000000000005");
        kinesisP.closedProcessedShards.add("shardId-000000000006");

        List<Shard> shards = new ArrayList<>();

        shards.add(createShard("shardId-000000000000", null, "0", "25", "1000")); // root, closed
        shards.add(createShard("shardId-000000000006", "shardId-000000000000", "0", "10", "1000")); // closed
        shards.add(createShard("shardId-000000000008", "shardId-000000000006", "0", "5", null)); // open
        shards.add(createShard("shardId-000000000009", "shardId-000000000006", "5", "10", null)); // open
        shards.add(createShard("shardId-000000000007", "shardId-000000000000", "10", "25", null)); // open
        shards.add(createShard("shardId-000000000001", null, "25", "50", null)); // root, open
        shards.add(createShard("shardId-000000000002", null, "50", "75", null)); // root, open
        shards.add(createShard("shardId-000000000003", null, "75", "90", null)); // root, open
        shards.add(createShard("shardId-000000000004", null, "90", "95", null)); // root, open
        shards.add(createShard("shardId-000000000005", null, "95", "100", "1000")); // root, closed
        shards.add(createShard("shardId-000000000010", "shardId-000000000005", "95", "98", null)); // open
        shards.add(createShard("shardId-000000000011", "shardId-000000000005", "98", "100", null)); // open

        for (int i = 0; i < 4; i++) {
            kinesisP.processorIndex = i;

            Map<String, Shard> assignment = kinesisP.getAssignment(shards);

            System.out.println("P index: " + i);
            System.out.println(assignment.keySet());
            System.out.println();
        }
    }

    private Shard createShard(String shardId, String parentShardId, String startKey, String endKey, String endSeqNumber) {
        return new Shard()
                .withShardId(shardId)
                .withParentShardId(parentShardId)
                .withHashKeyRange(new HashKeyRange().withStartingHashKey(startKey).withEndingHashKey(endKey))
                .withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("0").withEndingSequenceNumber(endSeqNumber));
    }
}

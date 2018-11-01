# hazelcast-jet-kinesis
Hazelcast Jet Connector for the Amazon Kinesis Streams v0.1 (draft)

## Background
[Hazelcast Jet](https://jet.hazelcast.org/) is the In-Memory Streaming and Fast Batch Processing Engine built on top of the Hazelcast In-Memory Data Grid (IMDG).

[Amazon Kinesis Data Streams (AKDS)](https://aws.amazon.com/kinesis/data-streams/getting-started/) is the AWS service for streaming data storage and processing. It has the functionality and semantics similar to Kafka so that you can think of it as a distributed publish-subscribe message queue provided as a service. 

## Configuration
You can either provide an explicit Client Id and Client Secret or it will use the [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html).  

## Scaling
AKDS partitions are called Shards. A Shard is the throughput unit of the Stream. It's append only and stores the data ordered by the arrival time. A single Shard can handle 1000 data records per second, or 1MB/sec for writes and 10 000 data records per second, or 2MB/sec for reads. There is also a new feature called [Enhanced Fan-Out](https://docs.aws.amazon.com/streams/latest/dev/introduction-to-enhanced-consumers.html) provided for additional cost. We may add a support for it in the future.

Generally, the scaling works in the way that there is a so-called Hash-Key Range which is split among Shards of a single Stream. Each emitted Data Record comes with a Partition Key which maps to the Hash Key Range and defines a Shard the Data Record belongs to. When you create the Stream you specify a number of Shards which, in turn, defines the entire throughput of the Stream. At this point the Hash Key Range is evenly distributed among Shards. Later you can scale up or scale down by splitting or merging these Shards. Consider the simplified example:

```
                        Stream "user-clicks"
-------------------------------------------------------------------
                            Hash Key Range
0                                                                 N
-------------------------------------------------------------------
1) Initially created, throughput X2
              
           Shard 01 [0..N/2]                            Shard 02 [N/2..N]
               
...

2) Splitting Shard 01 into Shards 03 & 04, throughput X3

           Shard 01* [0..N/2]                           Shard 02 [N/2..N]
                /\
               /  \
              /    \
             /      \
            /        \
Shard 03 [0-N/4]  Shard 04 [N/4-N/2]
 
...

3) Splitting Shard 02 into Shards 05 & 06, throughput X4

           Shard 01* [0..N/2]                             Shard 02* [N/2..N]
                /\                                            /\
               /  \                                          /  \
              /    \                                        /    \
             /      \                                      /      \
            /        \                                    /        \
Shard 03 [0..N/4]  Shard 04 [N/4..N/2]    Shard 05 [N/2..N/2+N/4]  Shard 06 [N/2+N/4..N]

...
4) Mergin Shard 02 into Shards 05 & 06, throughput X3
         
           Shard 01* [0..N/2]                             Shard 02* [N/2..N]
                /\                                            /\
               /  \                                          /  \
              /    \                                        /    \
             /      \                                      /      \
            /        \                                    /        \
Shard 03* [0..N/4]  Shard 04* [N/4..N/2]   Shard 05 [N/2..N/2+N/4]  Shard 06 [N/2+N/4..N]
            \        /  
             \      /
              \    /
               \  /
                \/    
          Shard 07 [0..N/2]   
...          
5) And so on, we can merge & split as much as we need (sure, there are some AWS limits for throughput)
```
     
As you can see, when Shards are split or merged, at the end they represent some kind of graph which we should properly traverse to make sure that the data is consumed in order. The rule is that once a split or merge happens (it can be detected through the API) you should 1st consume parent Shard (s) before proceeding with child Shard (s).     

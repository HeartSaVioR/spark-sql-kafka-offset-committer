# Kafka Offset committer for Spark structured streaming

Kafka Offset committer helps structured streaming query which uses Kafka Data Source to commit offsets which batch has been processed.

## Supported versions

Spark 2.4.x is supported: it only means you should link Spark 2.4.x when using this tool. That state formats across the Spark 2.x versions are supported.

The project doesn't support cross-scala versions: Scala 2.11.x is supported only.

## How to use

Kafka Offset committer is implemented as StreamingQueryListener. There're two approaches to enable streaming query listener:

1. Attach the instance of `KafkaOffsetCommitterListener` via below:

```scala
val listener = new KafkaOffsetCommitterListener()
spark.streams.addListener(listener)
```

2. Add `net.heartsavior.spark.KafkaOffsetCommitterListener` to the value of `spark.sql.streaming.streamingQueryListeners` in your Spark config.
(The value is separated by `,` so you can add multiple listeners if you have any other listeners.) 

Once the listener is set, you can add special option to Kafka data source options so that Kafka committer can see the `groupId` to commit:

```scala
spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribePattern", "topic[1-3]")
  .option("startingOffsets", "earliest")
  .option("kafka.consumer.commit.groupid", "groupId1")
  .load()
``` 

Manually specifying consumer group ID is needed, because Spark will assign unique consumer group ID to avoid multiple queries being conflicted to each other.
This also means, you may want to thoughtfully set the option and decide the name of group ID so that multiple queries don't use the same group ID for committing.   

## License

Copyright 2019 Jungtaek Lim "<kabhwan@gmail.com>"

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 

# Kafka offset committer for Spark structured streaming

[![CircleCI](https://circleci.com/gh/HeartSaVioR/spark-sql-kafka-offset-committer/tree/master.svg?style=svg)](https://circleci.com/gh/HeartSaVioR/spark-sql-kafka-offset-committer/tree/master)

Kafka offset committer helps structured streaming query which uses Kafka Data Source to commit offsets which batch has been processed.

This project is not for replacing checkpoint mechanism of Spark with Kafka's one. To provide full of "fault-tolerance" semantic, Spark has to take 100% of control of manipulating checkpoint, and Kafka data source is no exception. This project can be used to leverage Kafka ecosystem tools to track the committed offsets on Spark checkpoint, which is not possible solely with Spark.

This project is inspired by [SPARK-27549](https://issues.apache.org/jira/browse/SPARK-27549), which proposed to add this feature in Spark codebase, but the decision was taken as not include to Spark. You can call this project as a "follow-up" of SPARK-27549. This project is also inspired by [Spark Atlas Connector](https://github.com/hortonworks-spark/spark-atlas-connector) - SAC leverages Scala reflection to extract topic information from query execution. Kafka offset committer uses the same approach to extract Kafka parameters. Credits to everyone involved SPARK-27549 & SAC.

## Supported versions

Spark 2.4.x is supported: it only means you should link Spark 2.4.x when using this project.

The project doesn't support cross-scala versions: Scala 2.11.x is supported only.

## How to import

Add this to your maven pom.xml file. If you're using other builds like groovy or sbt or so, please import the artifact accordingly; groupId: `net.heartsavior.spark`, artifactId: `spark-sql-kafka-offset-committer`.

```
<dependency>
  <groupId>net.heartsavior.spark</groupId>
  <artifactId>spark-sql-kafka-offset-committer</artifactId>
  <version>0.1.0</version>
</dependency>
```

You can dynamically include jar file while submitting, via leveraging `--packages` option. `--packages net.heartsavior.spark:spark-sql-kafka-offset-committer:0.1.0`. You may want to add `--conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener` as well, since you're dynamically adding the jar, hence the class is not accessible in your uber jar.

## How to use

Kafka offset committer is implemented as StreamingQueryListener. There're two approaches to enable streaming query listener:

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

"kafka.consumer.commit.groupid" is the new config to specify consumer group ID to commit. Manually specifying consumer group ID is needed, because Spark will
assign unique consumer group ID to avoid multiple queries being conflicted to each other. This also means, you may want to thoughtfully set the option and
 decide the name of group ID so that multiple queries don't use the same group ID for committing.

Due to technical reason, the project uses reflection to extract options from query execution. Given we intercept Kafka parameters instead of source options
 of DataSource, adding "kafka." to option key is necessary and it brings unintended warning messages from Kafka side. (Sorry!) You can adjust your log4j config
to hide the warning messages.

Here's an example of command to run spark-shell with kafka committer listener being set, and simple query to read from Kafka topics and write to Kafka topic.

> command

```
./bin/spark-shell --master "local[3]" --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 --jars ./spark-sql-kafka-offset-committer-0.1.0-SNAPSHOT.jar --conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener
```

> query

```scala
val bootstrapServers = "localhost:9092"
val checkpointLocation = "/tmp/mykafkaaaaaaa"
val sourceTopics = Seq("truck_events_stream").mkString(",")
val sourceTopics2 = Seq("truck_speed_events_stream").mkString(",")

val targetTopic = "sparksinkstreaming"

val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option("subscribe", sourceTopics).option("startingOffsets", "earliest").option("kafka.consumer.commit.groupid", "spark-sql-kafka-offset-committer-test-1").load()

val df2 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option("subscribe", sourceTopics2).option("startingOffsets", "earliest").option("kafka.consumer.commit.groupid", "spark-sql-kafka-offset-committer-test-1").load()

val query = df.union(df2).writeStream.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option("checkpointLocation", checkpointLocation).option("topic", targetTopic).option("kafka.atlas.cluster.name", "sink").start()
```

> result

```
$ kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group spark-sql-kafka-offset-committer-test-1
Consumer group 'spark-sql-kafka-offset-committer-test-1' has no active members.

TOPIC                                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
truck_speed_events_stream                5          844553          844577          24              -               -               -
truck_speed_events_stream                2          675521          675540          19              -               -               -
truck_speed_events_stream                6          168828          168833          5               -               -               -
truck_speed_events_stream                3          337819          337827          8               -               -               -
truck_speed_events_stream                7          675566          675585          19              -               -               -
truck_speed_events_stream                4          168914          168919          5               -               -               -
truck_speed_events_stream                0          168894          168899          5               -               -               -
truck_speed_events_stream                8          675570          675589          19              -               -               -
truck_speed_events_stream                1          168917          168922          5               -               -               -
truck_events_stream                      0          3884586         3884695         109             -               -               -
truck_speed_events_stream                9          0               0               0               -               -               -
```

After stopping ingestion of records and waiting for query to fully process the records:

```
$ kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group spark-sql-kafka-offset-committer-test-1
Consumer group 'spark-sql-kafka-offset-committer-test-1' has no active members.

TOPIC                                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
truck_speed_events_stream                5          856338          856338          0               -               -               -
truck_speed_events_stream                2          684958          684958          0               -               -               -
truck_speed_events_stream                6          171186          171186          0               -               -               -
truck_speed_events_stream                3          342534          342534          0               -               -               -
truck_speed_events_stream                7          684998          684998          0               -               -               -
truck_speed_events_stream                4          171272          171272          0               -               -               -
truck_speed_events_stream                0          171255          171255          0               -               -               -
truck_speed_events_stream                8          684999          684999          0               -               -               -
truck_speed_events_stream                1          171276          171276          0               -               -               -
truck_events_stream                      0          3938820         3938820         0               -               -               -
truck_speed_events_stream                9          0               0               0               -               -               -
```


## License

Copyright 2019-2020 Jungtaek Lim "<kabhwan@gmail.com>"

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 

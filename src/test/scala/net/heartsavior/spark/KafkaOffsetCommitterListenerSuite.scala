/*
 * Copyright 2019 Jungtaek Lim "<kabhwan@gmail.com>"
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.heartsavior.spark

import java.io.File
import java.nio.file.Files
import java.util.Properties

import scala.collection.JavaConverters._

import net.heartsavior.spark.KafkaOffsetCommitterListener._
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition
import org.json4s.JsonAST.{JArray, JInt, JObject}
import org.json4s.jackson.JsonMethods.{compact, render}
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.streaming.{StreamingQuery, StreamTest}


class KafkaOffsetCommitterListenerSuite extends StreamTest {
  val brokerProps: Map[String, Object] = Map[String, Object]()
  var testUtils: KafkaTestUtils = _
  var adminClient: AdminClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(brokerProps)
    testUtils.setup()

    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, testUtils.brokerAddress)
    adminClient = AdminClient.create(props)
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
    }
    if (adminClient != null) {
      adminClient.close()
    }
    super.afterAll()
  }

  test("Run micro-batch query with Kafka source(s) - check offsets are committed") {
    def assertOffsetsAreCommited(
        groupId: String,
        topics: Seq[String],
        numPartitions: Int,
        expectedOffsetsPerPartition: Int): Unit = {
      val committedOffsets = adminClient.listConsumerGroupOffsets(groupId)
        .partitionsToOffsetAndMetadata().get().asScala
        .map { case (tp, offsetAndMetadata) => tp -> offsetAndMetadata.offset() }
        .toMap
      val expectedOffsets = topics.flatMap { topic =>
        (0 until numPartitions).map(new TopicPartition(topic, _) -> expectedOffsetsPerPartition)
      }.toMap
      assert(committedOffsets === expectedOffsets)
    }

    def assertOffsetsAreNotCommited(topics: Seq[String]): Unit = {
      val groups = adminClient.listConsumerGroups().all().get().asScala
      val committedTopics = groups.flatMap { grp =>
        adminClient.listConsumerGroupOffsets(grp.groupId())
          .partitionsToOffsetAndMetadata().get().asScala
          .map { case (tp, offsetAndMetadata) => tp -> offsetAndMetadata.offset() }
          .filter { case (_, offset) => offset >= 0 }
          .map { case (tp, _) => tp.topic() }
          .toSet
      }.toSet
      assert(Set.empty[String] === committedTopics.intersect(topics.toSet))
    }

    val listener = new KafkaOffsetCommitterListener()
    try {
      spark.streams.addListener(listener)

      val topicPrefix = "sparkread0"
      val brokerAddress = testUtils.brokerAddress

      val groupId1 = "groupId1"
      val (df1, topicsToRead1) = kafkaDfSubscribePattern(topicPrefix, brokerAddress, Some(groupId1))

      val groupId2 = "groupId2"
      val (df2, topicsToRead2) = kafkaDfSubscribe(topicPrefix, brokerAddress, Some(groupId2))

      val (df3, topicsToRead3) = kafkaDfAssign(topicPrefix, brokerAddress, Some(groupId1))

      val topicPrefix2 = "sparkread1"
      val (df4, topicsToRead4) = kafkaDfSubscribe(topicPrefix2, brokerAddress, None)

      val topicsToRead = topicsToRead1 ++ topicsToRead2 ++ topicsToRead3 ++ topicsToRead4

      val topicToWrite = "sparkwrite"
      val topics = topicsToRead ++ Seq(topicToWrite)

      val numPartitions = 10
      val msgsPerPartition = 100
      topics.toSet[String].foreach { ti =>
        testUtils.createTopic(ti, numPartitions, overwrite = true)
      }

      val (_, _, checkpointDir) = createTempDirectories

      val df = df1.union(df2).union(df3).union(df4)

      val query = toKafkaSink(df, brokerAddress, topicToWrite, checkpointDir.getAbsolutePath)

      try {
        sendMessages(topicsToRead, numPartitions, msgsPerPartition)
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          assertOffsetsAreCommited(groupId1, topicsToRead1 ++ topicsToRead3, numPartitions,
            msgsPerPartition)
          assertOffsetsAreCommited(groupId2, topicsToRead2, numPartitions, msgsPerPartition)
          assertOffsetsAreNotCommited(topicsToRead4)
        }
      } finally {
        query.stop()
      }
    } finally {
      spark.streams.removeListener(listener)
    }
  }

  private def kafkaDfSubscribePattern(
      topicNamePrefix: String,
      brokerAddress: String,
      commitGroupId: Option[String]): (Dataset[Row], Seq[String]) = {
    val topics = (1 to 3).map(idx => topicNamePrefix + idx)

    // test for 'subscribePattern'
    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("subscribePattern", topicNamePrefix + "[1-3]")
      .option("startingOffsets", "earliest")

    val df = commitGroupId match {
      case Some(gid) => stream.option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, gid).load()
      case _ => stream.load()
    }

    (df, topics)
  }

  private def kafkaDfSubscribe(
      topicNamePrefix: String,
      brokerAddress: String,
      commitGroupId: Option[String]): (Dataset[Row], Seq[String]) = {
    val topics = (4 to 5).map(idx => topicNamePrefix + idx)

    // test for 'subscribe'
    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("subscribe", topics.mkString(","))
      .option("startingOffsets", "earliest")

    val df = commitGroupId match {
      case Some(gid) => stream.option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, gid).load()
      case _ => stream.load()
    }

    (df, topics)
  }

  private def kafkaDfAssign(
      topicNamePrefix: String,
      brokerAddress: String,
      commitGroupId: Option[String]): (Dataset[Row], Seq[String]) = {
    val topics = (6 to 7).map(idx => topicNamePrefix + idx)
    // test for 'assign'
    val jsonToAssignTopicToRead = {
      val r = JObject.apply {
        topics.map {
          (_, JArray((0 until 10).map(JInt(_)).toList))
        }.toList
      }
      compact(render(r))
    }

    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("assign", jsonToAssignTopicToRead)
      .option("startingOffsets", "earliest")

    val df = commitGroupId match {
      case Some(gid) => stream.option(CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION, gid).load()
      case _ => stream.load()
    }

    (df, topics)
  }

  private def toKafkaSink(
      df: Dataset[Row],
      brokerAddress: String,
      topic: String,
      checkpointPath: String): StreamingQuery = {
    df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", topic)
      .option("checkpointLocation", checkpointPath)
      .start()
  }

  private def createTempDirectories: (File, File, File) = {
    val tempDir = Files.createTempDirectory("kafka-offset-committer-listener-suite-temp")

    val srcDir = new File(tempDir.toFile, "src")
    val destDir = new File(tempDir.toFile, "dest")
    val checkpointDir = new File(tempDir.toFile, "checkpoint")

    // remove temporary directory in shutdown
    org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
      new Runnable {
        override def run(): Unit = {
          FileUtils.forceDelete(tempDir.toFile)
        }
      }, 10)

    Files.createDirectories(srcDir.toPath)

    (srcDir, destDir, checkpointDir)
  }

  private def sendMessages(
      topicsToRead: Seq[String],
      numPartitions: Int,
      msgsPerPartition: Int): Unit = {
    topicsToRead.foreach { topic =>
      (0 until numPartitions).foreach { part =>
        val messages = (1 to msgsPerPartition).map(_.toString).toArray
        testUtils.sendMessages(topic, messages, Some(part))
      }
    }
  }
}

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

import java.time.Duration

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.kafka010.KafkaSourceInspector
import org.apache.spark.sql.streaming.StreamingQueryListener


class KafkaOffsetCommitterListener extends StreamingQueryListener with Logging {
  import KafkaOffsetCommitterListener._

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val query = SparkSession.active.streams.get(event.progress.id)
    if (query != null) {
      val exec = query match {
        case query: StreamingQueryWrapper => Some(query.streamingQuery.lastExecution)
        case query: StreamExecution => Some(query.lastExecution)
        case _ =>
          logWarning(s"Unexpected type of streaming query: ${query.getClass}")
          None
      }

      exec.foreach { ex =>
        val inspector = new KafkaSourceInspector(ex.executedPlan)
        val idxToKafkaParams = inspector.populateKafkaParams
        idxToKafkaParams.foreach { case (idx, params) =>
          params.get(CONFIG_KEY_GROUP_ID) match {
            case Some(groupId) =>
              val sourceProgress = event.progress.sources(idx)
              val tpToOffsets = inspector.partitionOffsets(sourceProgress.endOffset)

              val newParams = new scala.collection.mutable.HashMap[String, Object]
              newParams ++= params
              newParams += "group.id" -> groupId

              val kafkaConsumer = new KafkaConsumer[String, String](newParams.asJava)
              try {
                val offsetsToCommit = tpToOffsets.map { case (tp, offset) =>
                  (tp -> new OffsetAndMetadata(offset))
                }
                kafkaConsumer.commitSync(offsetsToCommit.asJava, Duration.ofSeconds(10))
              } finally {
                kafkaConsumer.close()
              }

            case None =>
          }
        }
      }
    } else {
      logWarning(s"Cannot find query ${event.progress.id} from active spark session!")
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}

object KafkaOffsetCommitterListener {
  val CONFIG_KEY_GROUP_ID = "consumer.commit.groupid"
  val CONFIG_KEY_GROUP_ID_DATA_SOURCE_OPTION = "kafka." + CONFIG_KEY_GROUP_ID
}

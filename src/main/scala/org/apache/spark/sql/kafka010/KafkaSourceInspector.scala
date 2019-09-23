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

package org.apache.spark.sql.kafka010

import scala.collection.JavaConverters._

import net.heartsavior.spark.ReflectionHelper
import org.apache.kafka.common.TopicPartition

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.{RDDScanExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, DataSourceV2ScanExec}
import org.apache.spark.sql.kafka010.{JsonUtils => KafkaJsonUtils}
import org.apache.spark.sql.sources.BaseRelation


class KafkaSourceInspector(sparkPlan: SparkPlan) {
  def populateKafkaParams: Map[Int, Map[String, Object]] = {
    sparkPlan.collectLeaves().zipWithIndex.flatMap { case (plan, idx) =>
      val paramsOpt = plan match {
        case r: RowDataSourceScanExec if isKafkaRelation(r.relation) =>
          extractKafkaParamsFromKafkaRelation(r.relation)
        case r: RDDScanExec =>
          extractKafkaParamsFromDataSourceV1(r)
        case r: DataSourceV2ScanExec =>
          extractSourceTopicsFromDataSourceV2(r)
        case _ => None
      }
      if (paramsOpt.isDefined) {
        Some((idx, paramsOpt.get))
      } else {
        None
      }
    }.map(elem => elem._1 -> elem._2).toMap
  }

  private def isKafkaRelation(rel: BaseRelation): Boolean = {
    rel match {
      case r: KafkaRelation => true
      case _ => false
    }
  }

  private def extractKafkaParamsFromKafkaRelation(rel: BaseRelation)
      : Option[Map[String, Object]] = {
    require(isKafkaRelation(rel))
    ReflectionHelper.reflectFieldWithContextClassloader[Map[String, String]](
      rel, "specifiedKafkaParams")
  }

  private def extractKafkaParamsFromDataSourceV1(r: RDDScanExec): Option[Map[String, Object]] = {
    extractKafkaParamsFromDataSourceV1(r.rdd)
  }

  def extractKafkaParamsFromDataSourceV1(r: RowDataSourceScanExec)
      : Option[Map[String, Object]] = {
    extractKafkaParamsFromDataSourceV1(r.rdd)
  }

  private def extractKafkaParamsFromDataSourceV1(
      rddContainingPartition: RDD[_]): Option[Map[String, Object]] = {
    rddContainingPartition.partitions.flatMap {
      case _: KafkaSourceRDDPartition =>
        extractKafkaParamsFromKafkaSourceRDDPartition(rddContainingPartition)
      case _ => None
    }.headOption
  }

  private def extractKafkaParamsFromKafkaSourceRDDPartition(
    rddContainingPartition: RDD[_]): Option[Map[String, Object]] = {
    def collectLeaves(rdd: RDD[_]): Seq[RDD[_]] = {
      // this method is being called with chains of MapPartitionRDDs
      // so this recursion won't stack up too much
      if (rdd.dependencies.isEmpty) {
        Seq(rdd)
      } else {
        rdd.dependencies.map(_.rdd).flatMap(collectLeaves)
      }
    }

    collectLeaves(rddContainingPartition).flatMap {
      case r: KafkaSourceRDD => Some(r)
      case _ => None
    }.headOption.flatMap(extractKafkaParamsFromKafkaSourceRDD).orElse(None)
  }

  private def extractKafkaParamsFromKafkaSourceRDD(
      rdd: KafkaSourceRDD): Option[Map[String, Object]] = {
    val map = ReflectionHelper.reflectFieldWithContextClassloader[java.util.Map[String, Object]](
      rdd, "executorKafkaParams")
    map.map(_.asScala.toMap).orElse(None)
  }

  def extractSourceTopicsFromDataSourceV2(r: DataSourceV2ScanExec): Option[Map[String, Object]] = {
    r.inputRDDs().flatMap { rdd =>
      rdd.partitions.flatMap {
        case e: DataSourceRDDPartition[_] => e.inputPartition match {
          case part: KafkaMicroBatchInputPartition =>
            Some(part.executorKafkaParams.asScala.toMap)
          case part: KafkaContinuousInputPartition =>
            Some(part.kafkaParams.asScala.toMap)
          case _ => None
        }
      }
    }.headOption
  }

  def partitionOffsets(str: String): Map[TopicPartition, Long] = {
    KafkaJsonUtils.partitionOffsets(str)
  }
}

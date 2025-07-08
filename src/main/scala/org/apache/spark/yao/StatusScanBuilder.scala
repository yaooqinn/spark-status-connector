package org.apache.spark.yao

import scala.collection.Seq

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.status.AppStatusStore
import org.apache.spark.yao.encoder.{ExecutorSummaryRecord, JobDataRecord, KeyValueRecord, RDDStorageInfoRecord, StageDataRecord}

case class StatusScanBuilder(
    store: AppStatusStore,
    tableName: String) extends ScanBuilder {

  private def toKeyValuePairs(pairs: Seq[(String, String)]): Array[KeyValueRecord] = {
    pairs.map { case (k, v) => KeyValueRecord(k, v) }.toArray
  }

  private def build0[T](values: Array[T]): StatusStoreScan[T] = {
    val encoder = StatusTable.encoder[T](tableName)
    new StatusStoreScan[T](values, encoder)
  }

  override def build(): Scan = {
    tableName match {
      case "application" =>
        build0(Array(store.applicationInfo()))
      case "executor" =>
        build0(store.executorList(false).map(e => ExecutorSummaryRecord(e)).toArray)

      case "spark_property" =>
        build0(toKeyValuePairs(store.environmentInfo().sparkProperties))
      case "hadoop_property" =>
        build0(toKeyValuePairs(store.environmentInfo().hadoopProperties))
      case "system_property" =>
        build0(toKeyValuePairs(store.environmentInfo().systemProperties))
      case "metrics_property" =>
        build0(toKeyValuePairs(store.environmentInfo().metricsProperties))
      case "runtime" =>
        val runtime = store.environmentInfo().runtime
        build0(
          Array(KeyValueRecord("javaVersion", runtime.javaVersion),
                KeyValueRecord("scalaVersion", runtime.scalaVersion),
                KeyValueRecord("javaHome", runtime.javaHome)))
      case "classpath" =>
        build0(toKeyValuePairs(store.environmentInfo().classpathEntries))

      case "job" =>
        build0(store.jobsList(null).map(j => JobDataRecord(j)).toArray)
      case "stage" =>
        val records = store.stageList(null).map(s => StageDataRecord(s)).toArray
        build0(records)
      case "active_stage" =>
        val records = store.activeStages().map(s => StageDataRecord(s)).toArray
        build0(records)

      case "rdd" =>
        val records = store.rddList(false).map(RDDStorageInfoRecord.apply).toArray
        build0(records)

      case _ =>
        throw new NoSuchTableException(Array(tableName))
    }
  }

  override def toString: String = "StatusScanBuilder"
}

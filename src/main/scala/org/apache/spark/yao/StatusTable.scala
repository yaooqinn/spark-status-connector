package org.apache.spark.yao

import java.util
import java.util.Locale

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.yao.util.CaseInsensitiveMap
import org.apache.spark.yao.StatusTable.tableSchema
import org.apache.spark.yao.encoder._

case class StatusTable(
    spark: SparkSession,
    name: String) extends Table with SupportsRead {

  override def schema(): StructType = tableSchema(name)

  override def capabilities(): util.Set[TableCapability] = util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    StatusScanBuilder(spark.sparkContext.statusStore, name.toLowerCase(Locale.ROOT))
  }
}

object StatusTable {
  private lazy val tableToEncoder: CaseInsensitiveMap[Encoder[_]] = {
    val temp = new CaseInsensitiveMap[Encoder[_]]()
    temp.put("application", APPLICATION)
    temp.put("executor", EXECUTOR)

    temp.put("hadoop_property", KV)
    temp.put("metrics_property", KV)
    temp.put("runtime", KV)
    temp.put("spark_property", KV)
    temp.put("system_property", KV)
    temp.put("classpath", JOB)

    temp.put("job", JOB)
    temp.put("stage", STAGE)
    temp.put("active_stage", STAGE)
    temp.put("rdd", RDD)

    temp
  }

  def tableExists(name: String, throwable: Boolean = false): Boolean = {
    if (tableToEncoder.containsKey(name)) {
      return true
    }
    if (throwable) {
      throw new NoSuchTableException(Array(name))
    } else {
      false
    }
  }

  def listTables(): Array[String] = {
    tableToEncoder.keySet().toArray(new Array[String](0))
  }

  def tableSchema(name: String): StructType = {
    encoder(name).schema
  }

  def encoder[T](name: String): Encoder[T] = {
    tableExists(name, throwable = true)
    tableToEncoder(name).asInstanceOf[Encoder[T]]
  }
}

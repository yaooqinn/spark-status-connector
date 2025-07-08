package org.apache.spark.yao.encoder.udt

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class ExecutorMetricsUDT extends UserDefinedType[ExecutorMetrics] {
  override def serialize(metrics: ExecutorMetrics): MapData = {
    val m = ExecutorMetricType.metricToOffset.map { case (key, _) =>
      UTF8String.fromString(key) -> metrics.getMetricValue(key)
    }
    ArrayBasedMapData(m)
  }

  override def deserialize(datum: Any): ExecutorMetrics = {
    val data = datum.asInstanceOf[MapData]
    val map = data.keyArray().toArray[UTF8String](StringType).map(_.toString)
      .zip(data.valueArray().toLongArray()).toMap
    new ExecutorMetrics(map)
  }

  override def userClass: Class[ExecutorMetrics] = classOf[ExecutorMetrics]

  override def sqlType: DataType = MapType(StringType, LongType, valueContainsNull = false)

  override def typeName: String = "executor_metrics"

  def stringifyValue(metrics: Any): String = metrics match {
    case m: ExecutorMetrics => serialize(m).toString
  }
}

object ExecutorMetricsUDT extends ExecutorMetricsUDT

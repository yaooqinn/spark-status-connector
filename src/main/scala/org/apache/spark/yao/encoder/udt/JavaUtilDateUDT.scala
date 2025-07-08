package org.apache.spark.yao.encoder.udt

import java.util.Date

import org.apache.spark.sql.types.{DataType, TimestampType, UserDefinedType}

class JavaUtilDateUDT extends UserDefinedType[Date] {
  override def serialize(date: Date): Long = {
    if (date == null) Long.MinValue else date.getTime
  }
  override def deserialize(datum: Any): Date = {
    datum match {
      case time: Long if time == Long.MinValue => null
      case time: Long => new Date(time)
      case _ => throw new IllegalArgumentException(s"Cannot deserialize $datum to java.util.Date")
    }
  }
  override def userClass: Class[Date] = classOf[Date]
  override def sqlType: DataType = TimestampType

  override def typeName: String = "jdate"

  def stringifyValue(date: Any): String = {
    date match {
      case d: Date => d.toInstant.toString
      case _ => null
    }
  }
}

object JavaUtilDateUDT extends JavaUtilDateUDT

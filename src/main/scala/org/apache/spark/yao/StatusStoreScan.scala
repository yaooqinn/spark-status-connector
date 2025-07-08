package org.apache.spark.yao

import org.apache.spark.sql.connector.read.LocalScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Serializer
import org.apache.spark.sql.catalyst.InternalRow

trait StatusStoreScanLike[T] extends LocalScan {
  def values: Array[T]
  def encoder: Encoder[T]
  private final def serializer: Serializer[T] = encoderFor(encoder).createSerializer()
  override final def readSchema(): StructType = encoder.schema
  override final def rows(): Array[InternalRow] = values.map(v => serializer(v))
}

case class StatusStoreScan[T](values: Array[T], encoder: Encoder[T])
  extends StatusStoreScanLike[T]
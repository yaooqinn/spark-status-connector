package org.apache.spark.yao.encoder.udt

import org.apache.spark.resource.ResourceInformation
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructType, UserDefinedType}
import org.apache.spark.sql.Row

class ResourceInformationUDT extends UserDefinedType[ResourceInformation] {
  override def serialize(resourceInfo: ResourceInformation): Row = {
    Row(resourceInfo.name, resourceInfo.addresses)
  }

  override def deserialize(datum: Any): ResourceInformation = {
    val row = datum.asInstanceOf[Row]
    new ResourceInformation(row.getString(0), row.getAs[Array[String]](1))
  }

  override def userClass: Class[ResourceInformation] = classOf[ResourceInformation]

  override def sqlType: DataType = {
    new StructType().add("name", StringType).add("addresses", ArrayType(StringType))
  }

  override def typeName: String = "resource_information"

  def stringifyValue(resourceInfo: Any): String = {
    resourceInfo match {
      case info: ResourceInformation => serialize(info).toString
      case _ => null
    }
  }
}

object ResourceInformationUDT extends ResourceInformationUDT

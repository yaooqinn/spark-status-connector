package org.apache.spark.yao.encoder.udt

import java.util.Date

import org.apache.spark.sql.types.{ArrayType, UDTRegistration}
import org.apache.spark.sql.Encoders
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.yao.BaseSuite

class JavaUtilDateUDTSuite extends BaseSuite {
  test("JavaUtilDateUDT serialization and deserialization") {
    val udt = new JavaUtilDateUDT()
    val date = new java.util.Date()

    // Serialize
    val serialized = udt.serialize(date)
    serialized shouldBe a [Long]

    // Deserialize
    val deserialized = udt.deserialize(serialized)
    deserialized shouldBe a [Date]
    deserialized.getTime shouldEqual date.getTime
  }

  test("JavaUtilDateUDT null handling") {
    val udt = new JavaUtilDateUDT()

    // Serialize null
    val serializedNull = udt.serialize(null)
    serializedNull shouldBe -9223372036854775808L
    // Deserialize null
    val deserializedNull = udt.deserialize(serializedNull)
    deserializedNull shouldBe null
  }

  test("UDT registration") {
    UDTRegistration.register(classOf[Date].getName, classOf[JavaUtilDateUDT].getName)
    UDTRegistration.getUDTFor(classOf[Date].getName).get shouldBe classOf[JavaUtilDateUDT]
    val appInfoSchema = Encoders.product[ApplicationInfo].schema
    val attemptInfoSchema = Encoders.product[ApplicationAttemptInfo].schema
    appInfoSchema("attempts").dataType shouldBe ArrayType(attemptInfoSchema)
  }
}

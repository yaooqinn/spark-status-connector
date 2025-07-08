package org.apache.spark.yao

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.status.api.v1.ApplicationInfo

package object encoder {

  val KV: Encoder[KeyValueRecord] = Encoders.product[KeyValueRecord]

  val APPLICATION: Encoder[ApplicationInfo] = Encoders.product[ApplicationInfo]
  val EXECUTOR: Encoder[ExecutorSummaryRecord] = Encoders.product[ExecutorSummaryRecord]

  val JOB: Encoder[JobDataRecord] = Encoders.product[JobDataRecord]
  val STAGE: Encoder[StageDataRecord] = Encoders.product[StageDataRecord]

  val RDD: Encoder[RDDStorageInfoRecord] = Encoders.product[RDDStorageInfoRecord]
}

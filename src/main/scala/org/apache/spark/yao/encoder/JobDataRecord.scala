package org.apache.spark.yao.encoder

import java.util.Date

import org.apache.spark.status.api.v1.JobData
import org.apache.spark.JobExecutionStatus

case class JobDataRecord(
    jobId: Int,
    name: String,
    description: Option[String],
    submissionTime: Option[Date],
    completionTime: Option[Date],
    stageIds: collection.Seq[Int],
    jobGroup: Option[String],
    jobTags: collection.Seq[String],
    status: JobExecutionStatus,
    numTasks: Int,
    numActiveTasks: Int,
    numCompletedTasks: Int,
    numSkippedTasks: Int,
    numFailedTasks: Int,
    numKilledTasks: Int,
    numCompletedIndices: Int,
    numActiveStages: Int,
    numCompletedStages: Int,
    numSkippedStages: Int,
    numFailedStages: Int,
    killedTasksSummary: Map[String, Int]) {
}

object JobDataRecord {
  def apply(jobData: JobData): JobDataRecord = {
    JobDataRecord(
      jobId = jobData.jobId,
      name = jobData.name,
      description = jobData.description,
      submissionTime = jobData.submissionTime,
      completionTime = jobData.completionTime,
      stageIds = jobData.stageIds,
      jobGroup = jobData.jobGroup,
      jobTags = jobData.jobTags,
      status = jobData.status,
      numTasks = jobData.numTasks,
      numActiveTasks = jobData.numActiveTasks,
      numCompletedTasks = jobData.numCompletedTasks,
      numSkippedTasks = jobData.numSkippedTasks,
      numFailedTasks = jobData.numFailedTasks,
      numKilledTasks = jobData.numKilledTasks,
      numCompletedIndices = jobData.numCompletedIndices,
      numActiveStages = jobData.numActiveStages,
      numCompletedStages = jobData.numCompletedStages,
      numSkippedStages = jobData.numSkippedStages,
      numFailedStages = jobData.numFailedStages,
      killedTasksSummary = jobData.killedTasksSummary)
  }
}

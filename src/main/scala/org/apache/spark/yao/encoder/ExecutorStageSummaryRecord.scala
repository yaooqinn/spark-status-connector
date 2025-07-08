package org.apache.spark.yao.encoder

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.status.api.v1.ExecutorStageSummary

case class ExecutorStageSummaryRecord (
    taskTime : Long,
    failedTasks : Int,
    succeededTasks : Int,
    killedTasks : Int,
    inputBytes : Long,
    inputRecords : Long,
    outputBytes : Long,
    outputRecords : Long,
    shuffleRead : Long,
    shuffleReadRecords : Long,
    shuffleWrite : Long,
    shuffleWriteRecords : Long,
    memoryBytesSpilled : Long,
    diskBytesSpilled : Long,
    peakMemoryMetrics: Option[ExecutorMetrics],
    isExcludedForStage: Boolean)

object ExecutorStageSummaryRecord {
  def apply(data: ExecutorStageSummary): ExecutorStageSummaryRecord = {
    new ExecutorStageSummaryRecord(
      taskTime = data.taskTime,
      failedTasks = data.failedTasks,
      succeededTasks = data.succeededTasks,
      killedTasks = data.killedTasks,
      inputBytes = data.inputBytes,
      inputRecords = data.inputRecords,
      outputBytes = data.outputBytes,
      outputRecords = data.outputRecords,
      shuffleRead = data.shuffleRead,
      shuffleReadRecords = data.shuffleReadRecords,
      shuffleWrite = data.shuffleWrite,
      shuffleWriteRecords = data.shuffleWriteRecords,
      memoryBytesSpilled = data.memoryBytesSpilled,
      diskBytesSpilled = data.diskBytesSpilled,
      peakMemoryMetrics = data.peakMemoryMetrics,
      isExcludedForStage = data.isExcludedForStage
    )
  }
}

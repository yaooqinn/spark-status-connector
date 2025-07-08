package org.apache.spark.yao.encoder

import java.util.Date

import scala.collection.Seq

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.status.api.v1._

case class StageDataRecord(
    status: StageStatus,
    stageId: Int,
    attemptId: Int,
    numTasks: Int,
    numActiveTasks: Int,
    numCompleteTasks: Int,
    numFailedTasks: Int,
    numKilledTasks: Int,
    numCompletedIndices: Int,

    submissionTime: Option[Date],
    firstTaskLaunchedTime: Option[Date],
    completionTime: Option[Date],
    failureReason: Option[String],

    executorDeserializeTime: Long,
    executorDeserializeCpuTime: Long,
    executorRunTime: Long,
    executorCpuTime: Long,
    resultSize: Long,
    jvmGcTime: Long,
    resultSerializationTime: Long,
    memoryBytesSpilled: Long,
    diskBytesSpilled: Long,
    peakExecutionMemory: Long,
    inputBytes: Long,
    inputRecords: Long,
    outputBytes: Long,
    outputRecords: Long,
    shuffleRemoteBlocksFetched: Long,
    shuffleLocalBlocksFetched: Long,
    shuffleFetchWaitTime: Long,
    shuffleRemoteBytesRead: Long,
    shuffleRemoteBytesReadToDisk: Long,
    shuffleLocalBytesRead: Long,
    shuffleReadBytes: Long,
    shuffleReadRecords: Long,
    shuffleCorruptMergedBlockChunks: Long,
    shuffleMergedFetchFallbackCount: Long,
    shuffleMergedRemoteBlocksFetched: Long,
    shuffleMergedLocalBlocksFetched: Long,
    shuffleMergedRemoteChunksFetched: Long,
    shuffleMergedLocalChunksFetched: Long,
    shuffleMergedRemoteBytesRead: Long,
    shuffleMergedLocalBytesRead: Long,
    shuffleRemoteReqsDuration: Long,
    shuffleMergedRemoteReqsDuration: Long,
    shuffleWriteBytes: Long,
    shuffleWriteTime: Long,
    shuffleWriteRecords: Long,

    name: String,
    description: Option[String],
    details: String,
    schedulingPool: String,

    rddIds: Seq[Int],
    accumulatorUpdates: Seq[AccumulableInfoRecord],
    tasks: Option[Map[Long, TaskDataRecord]],
    executorSummary: Option[Map[String, ExecutorStageSummaryRecord]],
    speculationSummary: Option[SpeculationStageSummaryRecord],
    killedTasksSummary: Map[String, Int],
    resourceProfileId: Int,
    peakExecutorMetrics: Option[ExecutorMetrics],
    taskMetricsDistributions: Option[TaskMetricDistributionsRecord],
    executorMetricsDistributions: Option[ExecutorMetricsDistributionsRecord],
    isShufflePushEnabled: Boolean,
    shuffleMergersCount: Int)

object StageDataRecord {
  def apply(data: StageData): StageDataRecord = {
    new StageDataRecord(
      status = data.status,
      stageId = data.stageId,
      attemptId = data.attemptId,
      numTasks = data.numTasks,
      numActiveTasks = data.numActiveTasks,
      numCompleteTasks = data.numCompleteTasks,
      numFailedTasks = data.numFailedTasks,
      numKilledTasks = data.numKilledTasks,
      numCompletedIndices = data.numCompletedIndices,

      submissionTime = data.submissionTime,
      firstTaskLaunchedTime = data.firstTaskLaunchedTime,
      completionTime = data.completionTime,
      failureReason = data.failureReason,

      executorDeserializeTime = data.executorDeserializeTime,
      executorDeserializeCpuTime = data.executorDeserializeCpuTime,
      executorRunTime = data.executorRunTime,
      executorCpuTime = data.executorCpuTime,
      resultSize = data.resultSize,
      jvmGcTime = data.jvmGcTime,
      resultSerializationTime = data.resultSerializationTime,
      memoryBytesSpilled = data.memoryBytesSpilled,
      diskBytesSpilled = data.diskBytesSpilled,
      peakExecutionMemory = data.peakExecutionMemory,
      inputBytes = data.inputBytes,
      inputRecords = data.inputRecords,
      outputBytes = data.outputBytes,
      outputRecords = data.outputRecords,
      shuffleRemoteBlocksFetched = data.shuffleRemoteBlocksFetched,
      shuffleLocalBlocksFetched = data.shuffleLocalBlocksFetched,
      shuffleFetchWaitTime = data.shuffleFetchWaitTime,
      shuffleRemoteBytesRead = data.shuffleRemoteBytesRead,
      shuffleRemoteBytesReadToDisk = data.shuffleRemoteBytesReadToDisk,
      shuffleLocalBytesRead = data.shuffleLocalBytesRead,
      shuffleReadBytes = data.shuffleReadBytes,
      shuffleReadRecords = data.shuffleReadRecords,
      shuffleCorruptMergedBlockChunks = data.shuffleCorruptMergedBlockChunks,
      shuffleMergedFetchFallbackCount = data.shuffleMergedFetchFallbackCount,
      shuffleMergedRemoteBlocksFetched = data.shuffleMergedRemoteBlocksFetched,
      shuffleMergedLocalBlocksFetched = data.shuffleMergedLocalBlocksFetched,
      shuffleMergedRemoteChunksFetched = data.shuffleMergedRemoteChunksFetched,
      shuffleMergedLocalChunksFetched = data.shuffleMergedLocalChunksFetched,
      shuffleMergedRemoteBytesRead = data.shuffleMergedRemoteBytesRead,
      shuffleMergedLocalBytesRead = data.shuffleMergedLocalBytesRead,
      shuffleRemoteReqsDuration = data.shuffleRemoteReqsDuration,
      shuffleMergedRemoteReqsDuration = data.shuffleMergedRemoteReqsDuration,
      shuffleWriteBytes = data.shuffleWriteBytes,
      shuffleWriteTime = data.shuffleWriteTime,
      shuffleWriteRecords = data.shuffleWriteRecords,

      name = data.name,
      description = data.description,
      details = data.details,
      schedulingPool = data.schedulingPool,

      rddIds = data.rddIds,
      accumulatorUpdates = data.accumulatorUpdates.map(AccumulableInfoRecord.apply),
      tasks = data.tasks.map(
        _.map { case (k, v) =>
          k -> TaskDataRecord(v)
        }
      ),
      executorSummary = data.executorSummary.map(
        _.map { case (k, v) =>
          k -> ExecutorStageSummaryRecord(v)
        }
      ),
      speculationSummary = data.speculationSummary.map(SpeculationStageSummaryRecord.apply),
      killedTasksSummary = data.killedTasksSummary,
      resourceProfileId = data.resourceProfileId,
      peakExecutorMetrics = data.peakExecutorMetrics,
      taskMetricsDistributions = data.taskMetricsDistributions.map(TaskMetricDistributionsRecord.apply),
      executorMetricsDistributions = data.executorMetricsDistributions.map(ExecutorMetricsDistributions.apply),
      isShufflePushEnabled = data.isShufflePushEnabled,
      shuffleMergersCount = data.shuffleMergersCount
    )
  }
}

case class AccumulableInfoRecord(
    id: Long,
    name: String,
    update: Option[String],
    value: String) {
}

object AccumulableInfoRecord {
  def apply(info: AccumulableInfo): AccumulableInfoRecord = {
    new AccumulableInfoRecord(
      id = info.id,
      name = info.name,
      update = info.update,
      value = info.value
    )
  }
}

case class SpeculationStageSummaryRecord(
    numTasks: Int,
    numActiveTasks: Int,
    numCompletedTasks: Int,
    numFailedTasks: Int,
    numKilledTasks: Int)

object SpeculationStageSummaryRecord {
  def apply(data: SpeculationStageSummary): SpeculationStageSummaryRecord = {
    new SpeculationStageSummaryRecord(
      numTasks = data.numTasks,
      numActiveTasks = data.numActiveTasks,
      numCompletedTasks = data.numCompletedTasks,
      numFailedTasks = data.numFailedTasks,
      numKilledTasks = data.numKilledTasks
    )
  }
}

case class InputMetricDistributionsRecord(
    bytesRead: IndexedSeq[Double],
    recordsRead: IndexedSeq[Double])

object InputMetricDistributionsRecord {
  def apply(data: InputMetricDistributions): InputMetricDistributionsRecord = {
    new InputMetricDistributionsRecord(
      bytesRead = data.bytesRead,
      recordsRead = data.recordsRead
    )
  }
}

case class OutputMetricDistributionsRecord(
    bytesWritten: IndexedSeq[Double],
    recordsWritten: IndexedSeq[Double])

object OutputMetricDistributionsRecord {
  def apply(data: OutputMetricDistributions): OutputMetricDistributionsRecord = {
    new OutputMetricDistributionsRecord(
      bytesWritten = data.bytesWritten,
      recordsWritten = data.recordsWritten
    )
  }
}
case class ShuffleReadMetricDistributionsRecord(
    readBytes: IndexedSeq[Double],
    readRecords: IndexedSeq[Double],
    remoteBlocksFetched: IndexedSeq[Double],
    localBlocksFetched: IndexedSeq[Double],
    fetchWaitTime: IndexedSeq[Double],
    remoteBytesRead: IndexedSeq[Double],
    remoteBytesReadToDisk: IndexedSeq[Double],
    totalBlocksFetched: IndexedSeq[Double],
    remoteReqsDuration: IndexedSeq[Double]) // TODO: shufflePushReadMetricsDist: ShufflePushReadMetricDistributions

object ShuffleReadMetricDistributionsRecord {
  def apply(data: ShuffleReadMetricDistributions): ShuffleReadMetricDistributionsRecord = {
    new ShuffleReadMetricDistributionsRecord(
      readBytes = data.readBytes,
      readRecords = data.readRecords,
      remoteBlocksFetched = data.remoteBlocksFetched,
      localBlocksFetched = data.localBlocksFetched,
      fetchWaitTime = data.fetchWaitTime,
      remoteBytesRead = data.remoteBytesRead,
      remoteBytesReadToDisk = data.remoteBytesReadToDisk,
      totalBlocksFetched = data.totalBlocksFetched,
      remoteReqsDuration = data.remoteReqsDuration
    )
  }
}

case class ShuffleWriteMetricDistributionsRecord(
    writeBytes: IndexedSeq[Double],
    writeTime: IndexedSeq[Double],
    writeRecords: IndexedSeq[Double])

object ShuffleWriteMetricDistributionsRecord {
  def apply(data: ShuffleWriteMetricDistributions): ShuffleWriteMetricDistributionsRecord = {
    new ShuffleWriteMetricDistributionsRecord(
      writeBytes = data.writeBytes,
      writeTime = data.writeTime,
      writeRecords = data.writeRecords
    )
  }
}

case class TaskMetricDistributionsRecord(
    quantiles: IndexedSeq[Double],

    duration: IndexedSeq[Double],
    executorDeserializeTime: IndexedSeq[Double],
    executorDeserializeCpuTime: IndexedSeq[Double],
    executorRunTime: IndexedSeq[Double],
    executorCpuTime: IndexedSeq[Double],
    resultSize: IndexedSeq[Double],
    jvmGcTime: IndexedSeq[Double],
    resultSerializationTime: IndexedSeq[Double],
    gettingResultTime: IndexedSeq[Double],
    schedulerDelay: IndexedSeq[Double],
    peakExecutionMemory: IndexedSeq[Double],
    memoryBytesSpilled: IndexedSeq[Double],
    diskBytesSpilled: IndexedSeq[Double],

    inputMetrics: InputMetricDistributionsRecord,
    outputMetrics: OutputMetricDistributionsRecord,
    shuffleReadMetrics: ShuffleReadMetricDistributionsRecord,
    shuffleWriteMetrics: ShuffleWriteMetricDistributionsRecord)

object TaskMetricDistributionsRecord {
  def apply(data: TaskMetricDistributions): TaskMetricDistributionsRecord = {
    new TaskMetricDistributionsRecord(
      quantiles = data.quantiles,

      duration = data.duration,
      executorDeserializeTime = data.executorDeserializeTime,
      executorDeserializeCpuTime = data.executorDeserializeCpuTime,
      executorRunTime = data.executorRunTime,
      executorCpuTime = data.executorCpuTime,
      resultSize = data.resultSize,
      jvmGcTime = data.jvmGcTime,
      resultSerializationTime = data.resultSerializationTime,
      gettingResultTime = data.gettingResultTime,
      schedulerDelay = data.schedulerDelay,
      peakExecutionMemory = data.peakExecutionMemory,
      memoryBytesSpilled = data.memoryBytesSpilled,
      diskBytesSpilled = data.diskBytesSpilled,

      inputMetrics = InputMetricDistributionsRecord(data.inputMetrics),
      outputMetrics = OutputMetricDistributionsRecord(data.outputMetrics),
      shuffleReadMetrics = ShuffleReadMetricDistributionsRecord(data.shuffleReadMetrics),
      shuffleWriteMetrics = ShuffleWriteMetricDistributionsRecord(data.shuffleWriteMetrics)
    )
  }
}

case class ExecutorPeakMetricsDistributionsRecord(
    quantiles: IndexedSeq[Double],
    executorMetrics: IndexedSeq[ExecutorMetrics])

object ExecutorPeakMetricsDistributionsRecord {
  def apply(data: ExecutorPeakMetricsDistributions): ExecutorPeakMetricsDistributionsRecord =
    new ExecutorPeakMetricsDistributionsRecord(
      quantiles = data.quantiles,
      executorMetrics = data.executorMetrics)
}

case class ExecutorMetricsDistributionsRecord(
    quantiles: IndexedSeq[Double],

    taskTime: IndexedSeq[Double],
    failedTasks: IndexedSeq[Double],
    succeededTasks: IndexedSeq[Double],
    killedTasks: IndexedSeq[Double],
    inputBytes: IndexedSeq[Double],
    inputRecords: IndexedSeq[Double],
    outputBytes: IndexedSeq[Double],
    outputRecords: IndexedSeq[Double],
    shuffleRead: IndexedSeq[Double],
    shuffleReadRecords: IndexedSeq[Double],
    shuffleWrite: IndexedSeq[Double],
    shuffleWriteRecords: IndexedSeq[Double],
    memoryBytesSpilled: IndexedSeq[Double],
    diskBytesSpilled: IndexedSeq[Double],
    peakMemoryMetrics: ExecutorPeakMetricsDistributionsRecord)

object ExecutorMetricsDistributions {
  def apply(data: ExecutorMetricsDistributions): ExecutorMetricsDistributionsRecord = {
    new ExecutorMetricsDistributionsRecord(
      quantiles = data.quantiles,

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
      peakMemoryMetrics = ExecutorPeakMetricsDistributionsRecord(data.peakMemoryMetrics)
    )
  }
}

case class TaskDataRecord(
    taskId: Long,
    index: Int,
    attempt: Int,
    partitionId: Int,
    launchTime: Date,
    resultFetchStart: Option[Date],
    duration: Option[Long],
    executorId: String,
    host: String,
    status: String,
    taskLocality: String,
    speculative: Boolean,
    accumulatorUpdates: Seq[AccumulableInfoRecord],
    errorMessage: Option[String] = None,
    taskMetrics: Option[TaskMetricsRecord] = None,
    executorLogs: Map[String, String],
    schedulerDelay: Long,
    gettingResultTime: Long)

object TaskDataRecord {
  def apply(data: TaskData): TaskDataRecord = {
    new TaskDataRecord(
      taskId = data.taskId,
      index = data.index,
      attempt = data.attempt,
      partitionId = data.partitionId,
      launchTime = data.launchTime,
      resultFetchStart = data.resultFetchStart,
      duration = data.duration,
      executorId = data.executorId,
      host = data.host,
      status = data.status,
      taskLocality = data.taskLocality,
      speculative = data.speculative,
      accumulatorUpdates = data.accumulatorUpdates.map(AccumulableInfoRecord.apply),
      errorMessage = data.errorMessage,
      taskMetrics = data.taskMetrics.map(TaskMetricsRecord.apply),
      executorLogs = data.executorLogs,
      schedulerDelay = data.schedulerDelay,
      gettingResultTime = data.gettingResultTime
    )
  }
}

case class TaskMetricsRecord(
    executorDeserializeTime: Long,
    executorDeserializeCpuTime: Long,
    executorRunTime: Long,
    executorCpuTime: Long,
    resultSize: Long,
    jvmGcTime: Long,
    resultSerializationTime: Long,
    memoryBytesSpilled: Long,
    diskBytesSpilled: Long,
    peakExecutionMemory: Long,
    inputMetrics: InputMetricsRecord,
    outputMetrics: OutputMetricsRecord,
    shuffleReadMetrics: ShuffleReadMetricsRecord,
    shuffleWriteMetrics: ShuffleWriteMetricsRecord)

object TaskMetricsRecord {
  def apply(data: TaskMetrics): TaskMetricsRecord = {
    new TaskMetricsRecord(
      executorDeserializeTime = data.executorDeserializeTime,
      executorDeserializeCpuTime = data.executorDeserializeCpuTime,
      executorRunTime = data.executorRunTime,
      executorCpuTime = data.executorCpuTime,
      resultSize = data.resultSize,
      jvmGcTime = data.jvmGcTime,
      resultSerializationTime = data.resultSerializationTime,
      memoryBytesSpilled = data.memoryBytesSpilled,
      diskBytesSpilled = data.diskBytesSpilled,
      peakExecutionMemory = data.peakExecutionMemory,
      inputMetrics = InputMetricsRecord(data.inputMetrics),
      outputMetrics = OutputMetricsRecord(data.outputMetrics),
      shuffleReadMetrics = ShuffleReadMetricsRecord(data.shuffleReadMetrics),
      shuffleWriteMetrics = ShuffleWriteMetricsRecord(data.shuffleWriteMetrics)
    )
  }
}

case class InputMetricsRecord(bytesRead: Long, recordsRead: Long)
object InputMetricsRecord {
  def apply(data: InputMetrics): InputMetricsRecord = {
    new InputMetricsRecord(
      bytesRead = data.bytesRead,
      recordsRead = data.recordsRead
    )
  }
}

case class OutputMetricsRecord(bytesWritten: Long, recordsWritten: Long)
object OutputMetricsRecord {
  def apply(data: OutputMetrics): OutputMetricsRecord = {
    new OutputMetricsRecord(
      bytesWritten = data.bytesWritten,
      recordsWritten = data.recordsWritten
    )
  }
}
case class ShuffleReadMetricsRecord(
    remoteBlocksFetched: Long,
    localBlocksFetched: Long,
    fetchWaitTime: Long,
    remoteBytesRead: Long,
    remoteBytesReadToDisk: Long,
    localBytesRead: Long,
    recordsRead: Long,
    remoteReqsDuration: Long) // TODO: shufflePushReadMetrics: ShufflePushReadMetrics

object ShuffleReadMetricsRecord {
  def apply(data: ShuffleReadMetrics): ShuffleReadMetricsRecord = {
    new ShuffleReadMetricsRecord(
      remoteBlocksFetched = data.remoteBlocksFetched,
      localBlocksFetched = data.localBlocksFetched,
      fetchWaitTime = data.fetchWaitTime,
      remoteBytesRead = data.remoteBytesRead,
      remoteBytesReadToDisk = data.remoteBytesReadToDisk,
      localBytesRead = data.localBytesRead,
      recordsRead = data.recordsRead,
      remoteReqsDuration = data.remoteReqsDuration
    )
  }
}

case class ShuffleWriteMetricsRecord(
    bytesWritten: Long,
    writeTime: Long,
    recordsWritten: Long)
object ShuffleWriteMetricsRecord {
  def apply(data: ShuffleWriteMetrics): ShuffleWriteMetricsRecord = {
    new ShuffleWriteMetricsRecord(
      bytesWritten = data.bytesWritten,
      writeTime = data.writeTime,
      recordsWritten = data.recordsWritten
    )
  }
}
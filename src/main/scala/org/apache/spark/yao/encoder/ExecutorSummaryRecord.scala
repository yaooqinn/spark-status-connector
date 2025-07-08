package org.apache.spark.yao.encoder

import java.util.Date

import scala.collection.Map

import org.apache.spark.resource.ResourceInformation
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.status.api.v1.ExecutorSummary

case class ExecutorSummaryRecord(
    id: String,
    hostPort: String,
    isActive: Boolean,
    rddBlocks: Int,
    memoryUsed: Long,
    diskUsed: Long,
    totalCores: Int,
    maxTasks: Int,
    activeTasks: Int,
    failedTasks: Int,
    completedTasks: Int,
    totalTasks: Int,
    totalDuration: Long,
    totalGCTime: Long,
    totalInputBytes: Long,
    totalShuffleRead: Long,
    totalShuffleWrite: Long,
    maxMemory: Long,
    addTime: Date,
    removeTime: Option[Date],
    removeReason: Option[String],
    executorLogs: Map[String, String],
    memoryMetrics: Option[MemoryMetricsRecord],
    peakMemoryMetrics: Option[ExecutorMetrics],
    attributes: Map[String, String],
    resources: Map[String, ResourceInformation],
    resourceProfileId: Int,
    isExcluded: Boolean,
    excludedInStages: Set[Int])

object ExecutorSummaryRecord {
  def apply(executorSummary: ExecutorSummary): ExecutorSummaryRecord = {
    new ExecutorSummaryRecord(
      executorSummary.id,
      executorSummary.hostPort,
      executorSummary.isActive,
      executorSummary.rddBlocks,
      executorSummary.memoryUsed,
      executorSummary.diskUsed,
      executorSummary.totalCores,
      executorSummary.maxTasks,
      executorSummary.activeTasks,
      executorSummary.failedTasks,
      executorSummary.completedTasks,
      executorSummary.totalTasks,
      executorSummary.totalDuration,
      executorSummary.totalGCTime,
      executorSummary.totalInputBytes,
      executorSummary.totalShuffleRead,
      executorSummary.totalShuffleWrite,
      executorSummary.maxMemory,
      executorSummary.addTime,
      executorSummary.removeTime,
      executorSummary.removeReason,
      executorSummary.executorLogs,
      executorSummary.memoryMetrics.map { m => MemoryMetricsRecord(
        m.usedOnHeapStorageMemory,
        m.usedOffHeapStorageMemory,
        m.totalOnHeapStorageMemory,
        m.totalOffHeapStorageMemory)
      },
      executorSummary.peakMemoryMetrics,
      executorSummary.attributes,
      executorSummary.resources,
      executorSummary.resourceProfileId,
      executorSummary.isExcluded,
      executorSummary.excludedInStages)
  }
}

case class MemoryMetricsRecord(
    usedOnHeapStorageMemory: Long,
    usedOffHeapStorageMemory: Long,
    totalOnHeapStorageMemory: Long,
    totalOffHeapStorageMemory: Long)

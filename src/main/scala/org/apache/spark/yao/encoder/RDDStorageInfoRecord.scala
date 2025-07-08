package org.apache.spark.yao.encoder

import scala.collection.Seq

import org.apache.spark.status.api.v1.{RDDDataDistribution, RDDPartitionInfo, RDDStorageInfo}

case class RDDStorageInfoRecord(
    id: Int,
    name: String,
    numPartitions: Int,
    numCachedPartitions: Int,
    storageLevel: String,
    memoryUsed: Long,
    diskUsed: Long,
    dataDistribution: Option[Seq[RDDDataDistributionRecord]],
    partitions: Option[Seq[RDDPartitionInfoRecord]])

object RDDStorageInfoRecord {
  def apply(data: RDDStorageInfo): RDDStorageInfoRecord = {
    new RDDStorageInfoRecord(
      id = data.id,
      name = data.name,
      numPartitions = data.numPartitions,
      numCachedPartitions = data.numCachedPartitions,
      storageLevel = data.storageLevel,
      memoryUsed = data.memoryUsed,
      diskUsed = data.diskUsed,
      dataDistribution = data.dataDistribution.map(_.map(RDDDataDistributionRecord.apply)),
      partitions = data.partitions.map(_.map(RDDPartitionInfoRecord.apply))
    )
  }
}

case class RDDDataDistributionRecord(
    address: String,
    memoryUsed: Long,
    memoryRemaining: Long,
    diskUsed: Long,
    onHeapMemoryUsed: Option[Long],
    offHeapMemoryUsed: Option[Long],
    onHeapMemoryRemaining: Option[Long],
    offHeapMemoryRemaining: Option[Long])

object RDDDataDistributionRecord {
  def apply(data: RDDDataDistribution): RDDDataDistributionRecord = {
    new RDDDataDistributionRecord(
      address = data.address,
      memoryUsed = data.memoryUsed,
      memoryRemaining = data.memoryRemaining,
      diskUsed = data.diskUsed,
      onHeapMemoryUsed = data.onHeapMemoryUsed,
      offHeapMemoryUsed = data.offHeapMemoryUsed,
      onHeapMemoryRemaining = data.onHeapMemoryRemaining,
      offHeapMemoryRemaining = data.offHeapMemoryRemaining
    )
  }
}

case class RDDPartitionInfoRecord(
    blockName: String,
    storageLevel: String,
    memoryUsed: Long,
    diskUsed: Long,
    executors: collection.Seq[String])

object RDDPartitionInfoRecord {
  def apply(data: RDDPartitionInfo): RDDPartitionInfoRecord = {
    new RDDPartitionInfoRecord(
      blockName = data.blockName,
      storageLevel = data.storageLevel,
      memoryUsed = data.memoryUsed,
      diskUsed = data.diskUsed,
      executors = data.executors
    )
  }
}



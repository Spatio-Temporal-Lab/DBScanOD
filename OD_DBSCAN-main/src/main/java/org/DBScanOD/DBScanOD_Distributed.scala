package org.DBScanOD.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.{LongAccumulator, DoubleAccumulator}
import scala.collection.mutable
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.util.Date

object DBScanOD_Distributed {
  def train(data: RDD[ODPoint],
            distanceEps: Double,
            timeEps: Long,
            minPoints: Int,
            numberOfPartitions: Int,
            cubeMultiplierDist: Double = 1.0,
            cubeMultiplierTime: Double = 1.0,
            alpha: Double = 0.5): DBScanOD_Distributed = {
    new DBScanOD_Distributed(distanceEps, timeEps, minPoints, numberOfPartitions, null)
      .train(data, cubeMultiplierDist, cubeMultiplierTime, alpha)
  }
}

class DBScanOD_Distributed private(val distanceEps: Double,
                                   val timeEps: Long,
                                   val minPoints: Int,
                                   val numberOfPartitions: Int,
                                   @transient private val labeledPartitionedPoints: RDD[(Int, DBScanLabeledODPoint)])
  extends Serializable {

  type ClusterID = (Int, Int)

  var partitionTime: Long = 0L
  var globalMergeTime: Long = 0L
  var partitionSkewness: Double = 0.0
  var taskSkewness: Double = 0.0
  var replicationRatio: Double = 0.0
  var sortTime: Long = 0L
  var indexTime: Long = 0L
  var neighborSearchTime: Long = 0L
  var clusterExpansionTime: Long = 0L
  var indexSpaceKb: Double = 0.0
  var totalTaskTime: Long = 0L
  var totalUniquePoints: Long = 0L

  def labeledPoints: RDD[DBScanLabeledODPoint] = labeledPartitionedPoints.values

  private def train(data: RDD[ODPoint],
                    cubeMultiplierDist: Double,
                    cubeMultiplierTime: Double,
                    alpha: Double): DBScanOD_Distributed = {
    val sc = data.context
    val timeFormat = new SimpleDateFormat("HH:mm:ss")

    val sortTimeAcc = sc.longAccumulator("SortTime")
    val indexTimeAcc = sc.longAccumulator("IndexTime")
    val neighborSearchTimeAcc = sc.longAccumulator("NeighborSearchTime")
    val clusterExpansionTimeAcc = sc.longAccumulator("ClusterExpansionTime")
    val indexSpaceKbAcc = sc.doubleAccumulator("IndexSpaceKb")
    val taskTimesAcc = sc.collectionAccumulator[Long]("TaskExecutionTimes")
    val processedPointsAcc = sc.longAccumulator("ProcessedODPoints")

    println(s"[${timeFormat.format(new Date())}] [阶段 1/4] 正在计算 3D 空间网格密度并构建 KL 启发式图划分...")
    val tPartitionStart = System.currentTimeMillis()
    val partResult = PartitionUtils_OD.getPartitions(
      data, numberOfPartitions, distanceEps, timeEps, cubeMultiplierDist, cubeMultiplierTime, alpha
    )
    val tPartitionEnd = System.currentTimeMillis()
    val calculatedPartitionTime = tPartitionEnd - tPartitionStart
    println(s"[${timeFormat.format(new Date())}] [阶段 1/4] 空间图划分与 MBR 计算完成，耗时: $calculatedPartitionTime ms")

    val gridToPartBroadcast = data.context.broadcast(partResult.gridToPartition)
    val mbrBroadcast = data.context.broadcast(partResult.partitionMBRs)
    val deltaDist = partResult.deltaDist
    val deltaTime = partResult.deltaTime

    val safeDistEps = distanceEps + 1e-9
    val safeTimeEps = timeEps.toDouble + 1e-9

    def getMainPartition(point: ODPoint): Option[Int] = {
      if (point == null) return None
      val gx = math.floor((point.startLon + 1e-9) / deltaDist).toInt
      val gy = math.floor((point.startLat + 1e-9) / deltaDist).toInt
      val gt = math.floor((point.startTime + 1e-9) / deltaTime).toInt
      gridToPartBroadcast.value.get(PartitionUtils_OD.CubeId(gx, gy, gt))
    }

    def getValidPartitions(point: ODPoint): Set[Int] = {
      if (point == null) return Set.empty
      val mainPartOpt = getMainPartition(point)

      val minGx = math.floor((point.startLon - safeDistEps + 1e-9) / deltaDist).toInt
      val maxGx = math.floor((point.startLon + safeDistEps + 1e-9) / deltaDist).toInt
      val minGy = math.floor((point.startLat - safeDistEps + 1e-9) / deltaDist).toInt
      val maxGy = math.floor((point.startLat + safeDistEps + 1e-9) / deltaDist).toInt
      val minGt = math.floor((point.startTime - safeTimeEps + 1e-9) / deltaTime).toInt
      val maxGt = math.floor((point.startTime + safeTimeEps + 1e-9) / deltaTime).toInt

      val candidateParts = mutable.Set[Int]()
      val map = gridToPartBroadcast.value
      for (x <- minGx to maxGx; y <- minGy to maxGy; t <- minGt to maxGt) {
        map.get(PartitionUtils_OD.CubeId(x, y, t)).foreach(candidateParts.add)
      }

      val validParts = candidateParts.filter { p =>
        mainPartOpt.contains(p) || {
          mbrBroadcast.value.get(p) match {
            case Some(mbr) => mbr.containsExpanded(point.endLon, point.endLat, point.endTime, distanceEps, timeEps)
            case None => false
          }
        }
      }

      if (validParts.isEmpty && mainPartOpt.isDefined) {
        Set(mainPartOpt.get)
      } else {
        validParts.toSet
      }
    }

    println(s"[${timeFormat.format(new Date())}] [阶段 2/4] 正在计算双端过滤后的重叠备份点与分区负载...")
    val duplicated: RDD[(Int, ODPoint)] = data.filter(_ != null).flatMap { point =>
      val validParts = getValidPartitions(point)
      validParts.map(id => (id, point))
    }

    duplicated.cache()
    val partitionSizes = duplicated.mapValues(_ => 1L).reduceByKey(_ + _).collect()
    val sizes = partitionSizes.map(_._2)

    val maxPartitionSize = if (sizes.nonEmpty) sizes.max else 0L
    val avgPartitionSize = if (sizes.nonEmpty) sizes.sum.toDouble / sizes.length else 1.0
    val calculatedPartitionSkewness = if (avgPartitionSize > 0) maxPartitionSize.toDouble / avgPartitionSize else 1.0
    val totalPartitionedPoints = sizes.sum

    val totalUniquePoints = data.map(_ => 1L).reduce(_ + _)
    val calculatedReplicationRatio = if (totalUniquePoints > 0) totalPartitionedPoints.toDouble / totalUniquePoints else 1.0

    val clustered: RDD[(Int, DBScanLabeledODPoint)] = duplicated
      .groupByKey(numberOfPartitions)
      .flatMapValues { points =>
        val tTaskStart = System.currentTimeMillis()
        val localEngine = new LocalDBScan_OD(
          distanceEps, timeEps, minPoints,
          sortTimeAcc, indexTimeAcc, neighborSearchTimeAcc, clusterExpansionTimeAcc, indexSpaceKbAcc,
          processedPointsAcc
        )
        val res = localEngine.fit(points)
        val taskDur = System.currentTimeMillis() - tTaskStart
        taskTimesAcc.add(taskDur)
        res
      }

    clustered.cache()

    val totalToProcess = totalPartitionedPoints
    @volatile var isLocalClusteringFinished = false

    val monitorThread = new Thread(new Runnable {
      override def run(): Unit = {
        var lastReportedStep = 0
        var zeroProgressReportCount = 0
        println(s"[${timeFormat.format(new Date())}] [阶段 3/4] 已提交分布式 Task，开始 Executor 端局部聚类计算 (总计算点数: $totalToProcess)...")

        while (!isLocalClusteringFinished && lastReportedStep < 10000) {
          try {
            Thread.sleep(2000)
            val currentProcessed = processedPointsAcc.value

            if (currentProcessed == 0) {
              zeroProgressReportCount += 1
              if (zeroProgressReportCount % 3 == 0) {
                val currentTime = timeFormat.format(new Date())
                println(s"[$currentTime] [局部OD聚类进度] 正在等待各个 Executor 完成数据 Shuffle 传输、确定性排序与网格索引构建...")
              }
            } else if (totalToProcess > 0) {
              val currentStep = math.min(10000, ((currentProcessed.toDouble / totalToProcess) * 10000).toInt)
              while (lastReportedStep + 1 <= currentStep && lastReportedStep < 10000) {
                lastReportedStep += 1
                val currentPercent = lastReportedStep / 100.0
                val currentTime = timeFormat.format(new Date())
                println(f"[$currentTime] [局部OD聚类进度] 已完成: $currentPercent%.2f%% ($currentProcessed%,d / $totalToProcess%,d OD Points)")
              }
            }
          } catch {
            case _: InterruptedException =>
          }
        }
      }
    })

    monitorThread.setDaemon(true)
    monitorThread.start()

    val totalClusteredCount = clustered.count()

    isLocalClusteringFinished = true
    monitorThread.interrupt()
    println(s"[${timeFormat.format(new Date())}] [局部OD聚类进度] 已完成: 100.00% ($totalToProcess / $totalToProcess OD Points)\n")

    println(s"[${timeFormat.format(new Date())}] [阶段 4/4] 正在执行跨分区全局合并与边界冲突裁决...")
    val tMergeStart = System.currentTimeMillis()

    val adjacencies: Array[((Int, Int), (Int, Int))] = clustered.flatMap { case (partition, point) =>
      if (point != null && point.od != null && point.flag != Flag.Noise) {
        val overlaps = getValidPartitions(point.od)
        if (overlaps.size > 1) {
          Some((point.od, (partition, point.cluster, point.flag)))
        } else None
      } else None
    }.groupByKey(numberOfPartitions).flatMap { case (od, labels) =>
      val labelsArr = labels.toArray
      val edges = mutable.Set[((Int, Int), (Int, Int))]()
      for (i <- 0 until labelsArr.length; j <- i + 1 until labelsArr.length) {
        val (p1, c1, f1) = labelsArr(i)
        val (p2, c2, f2) = labelsArr(j)
        if (f1 == Flag.Core || f2 == Flag.Core) {
          edges.add(((p1, c1), (p2, c2)))
        }
      }
      edges
    }.distinct().collect()

    val localClusterIds = clustered.filter(p => p._2 != null && p._2.flag != Flag.Noise)
      .mapValues(_.cluster).distinct().collect().toList

    val adjacenciesGraph = adjacencies.foldLeft(DBScanGraph_OD[ClusterID]())({
      case (graph, (from, to)) => graph.connect(from, to)
    })

    val (_, clusterIdToGlobalId) = localClusterIds.foldLeft((0, Map[ClusterID, Int]()))({
      case ((id, map), clusterId) =>
        map.get(clusterId) match {
          case None =>
            val nextId = id + 1
            val connectedClusters = adjacenciesGraph.getConnected(clusterId) + clusterId
            val toAdd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toAdd)
          case Some(_) => (id, map)
        }
    })

    val clusterIdsBroadcast = sc.broadcast(clusterIdToGlobalId)

    val labeledInner = clustered.filter { case (partition, point) =>
      if (point != null && point.od != null) {
        val mainPartOpt = getMainPartition(point.od)
        val overlaps = getValidPartitions(point.od)
        mainPartOpt.contains(partition) && overlaps.size == 1
      } else false
    }.map { case (partition, point) =>
      if (point.flag != Flag.Noise) point.cluster = clusterIdsBroadcast.value.getOrElse((partition, point.cluster), DBScanLabeledODPoint.Unknown)
      (partition, point)
    }

    val labeledOuter = clustered.flatMap { case (partition, point) =>
      if (point != null && point.od != null) {
        val overlaps = getValidPartitions(point.od)
        if (overlaps.size > 1) {
          Some((point.od, (partition, point)))
        } else None
      } else None
    }.groupByKey(numberOfPartitions).map { case (od, partitionPoints) =>
      val winner = partitionPoints.foldLeft(null: DBScanLabeledODPoint) { case (prev, (part, point)) =>
        if (point != null && point.flag != Flag.Noise) {
          point.cluster = clusterIdsBroadcast.value.getOrElse((part, point.cluster), DBScanLabeledODPoint.Unknown)
        }
        if (prev == null) point
        else {
          val (winFlag, winCluster) = (prev.flag, point.flag) match {
            case (Flag.Core, Flag.Core) => (Flag.Core, math.min(prev.cluster, point.cluster))
            case (Flag.Core, _) => (Flag.Core, prev.cluster)
            case (_, Flag.Core) => (Flag.Core, point.cluster)
            case (Flag.Border, Flag.Border) =>
              val c1 = if (prev.cluster > 0) prev.cluster else Int.MaxValue
              val c2 = if (point.cluster > 0) point.cluster else Int.MaxValue
              val minC = math.min(c1, c2)
              (Flag.Border, if (minC == Int.MaxValue) DBScanLabeledODPoint.Unknown else minC)
            case (Flag.Border, _) => (Flag.Border, prev.cluster)
            case (_, Flag.Border) => (Flag.Border, point.cluster)
            case _ => (Flag.Noise, DBScanLabeledODPoint.Unknown)
          }
          prev.flag = winFlag
          prev.cluster = winCluster
          prev
        }
      }
      val mainPart = getMainPartition(od).getOrElse(0)
      (mainPart, winner)
    }

    val finalUnionRDD = labeledInner.union(labeledOuter).cache()

    finalUnionRDD.count()
    val tMergeEnd = System.currentTimeMillis()
    val calculatedMergeTime = tMergeEnd - tMergeStart

    val taskTimes = taskTimesAcc.value.asScala.map(_.toLong).toArray
    val maxTaskTime = if (taskTimes.nonEmpty) taskTimes.max else 0L
    val avgTaskTime = if (taskTimes.nonEmpty) taskTimes.sum.toDouble / taskTimes.length else 1.0
    val calculatedTaskSkewness = if (avgTaskTime > 0) maxTaskTime.toDouble / avgTaskTime else 1.0

    val resultObj = new DBScanOD_Distributed(distanceEps, timeEps, minPoints, numberOfPartitions, finalUnionRDD)
    resultObj.partitionTime = calculatedPartitionTime
    resultObj.globalMergeTime = calculatedMergeTime
    resultObj.partitionSkewness = calculatedPartitionSkewness
    resultObj.taskSkewness = calculatedTaskSkewness
    resultObj.replicationRatio = calculatedReplicationRatio

    val numPartitions = sizes.length
    resultObj.sortTime = if (numPartitions > 0) sortTimeAcc.value / numPartitions else 0L
    resultObj.indexTime = if (numPartitions > 0) indexTimeAcc.value / numPartitions else 0L
    resultObj.neighborSearchTime = if (numPartitions > 0) neighborSearchTimeAcc.value / numPartitions else 0L
    resultObj.clusterExpansionTime = if (numPartitions > 0) clusterExpansionTimeAcc.value / numPartitions else 0L
    resultObj.indexSpaceKb = if (numPartitions > 0) indexSpaceKbAcc.value / numPartitions else 0.0
    resultObj.totalTaskTime = taskTimes.sum
    resultObj.totalUniquePoints = totalUniquePoints

    resultObj
  }
}
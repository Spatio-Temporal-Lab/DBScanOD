package org.DBScanOD.spark

import scala.collection.mutable
import scala.math._
import org.apache.spark.util.{LongAccumulator, DoubleAccumulator}

class LocalDBScan_OD(
                      distanceEps: Double,
                      timeEps: Long,
                      minPoints: Int,
                      sortTimeAcc: LongAccumulator = null,
                      indexTimeAcc: LongAccumulator = null,
                      neighborSearchTimeAcc: LongAccumulator = null,
                      clusterExpansionTimeAcc: LongAccumulator = null,
                      indexSpaceKbAcc: DoubleAccumulator = null,
                      processedPointsAcc: LongAccumulator = null
                    ) extends Serializable {

  var neighborSearchTime: Long = 0L
  var clusterExpansionTime: Long = 0L
  private var localProcessedCount: Int = 0

  @inline private def markVisited(p: DBScanLabeledODPoint): Unit = {
    p.visited = true
    if (processedPointsAcc != null) {
      localProcessedCount += 1
      if (localProcessedCount >= 100) {
        processedPointsAcc.add(localProcessedCount)
        localProcessedCount = 0
      }
    }
  }

  def fit(points: Iterable[ODPoint]): Iterable[DBScanLabeledODPoint] = {
    if (points == null || points.isEmpty) return Iterable.empty

    val tSortStart = System.currentTimeMillis()
    val labeledPoints = points
      .filter(p => p != null)
      .map(new DBScanLabeledODPoint(_))
      .filter(p => p.od != null)
      .toArray
      .sortBy(p => (p.startTime, p.startLon, p.startLat, p.endTime, p.endLon, p.endLat))
    val sortDur = System.currentTimeMillis() - tSortStart
    if (sortTimeAcc != null) sortTimeAcc.add(sortDur)

    val tIndexStart = System.currentTimeMillis()
    val grid = mutable.Map[(Int, Int), mutable.ArrayBuffer[DBScanLabeledODPoint]]()
    labeledPoints.foreach { p =>
      val gx = floor((p.startLon + 1e-11) / distanceEps).toInt
      val gy = floor((p.startLat + 1e-11) / distanceEps).toInt
      grid.getOrElseUpdate((gx, gy), mutable.ArrayBuffer()) += p
    }
    val sortedGrid = grid.mapValues(_.toArray.sortBy(_.startTime)).toMap
    val indexDur = System.currentTimeMillis() - tIndexStart
    if (indexTimeAcc != null) indexTimeAcc.add(indexDur)

    val G = sortedGrid.size
    val N = labeledPoints.length
    val localIndexSpaceKb = (G * 80.0 + N * 4.0) / 1024.0
    if (indexSpaceKbAcc != null) indexSpaceKbAcc.add(localIndexSpaceKb)

    var clusterId = 0
    labeledPoints.foreach { p =>
      if (!p.visited) {
        markVisited(p)
        val neighbors = findNeighbors(p, sortedGrid)
        if (neighbors.size < minPoints) {
          p.flag = Flag.Noise
        } else {
          clusterId += 1
          expandCluster(p, neighbors, clusterId, sortedGrid)
        }
      }
    }

    if (processedPointsAcc != null && localProcessedCount > 0) {
      processedPointsAcc.add(localProcessedCount)
      localProcessedCount = 0
    }

    if (neighborSearchTimeAcc != null) neighborSearchTimeAcc.add(neighborSearchTime)
    if (clusterExpansionTimeAcc != null) clusterExpansionTimeAcc.add(clusterExpansionTime)

    labeledPoints
  }

  private def findNeighbors(target: DBScanLabeledODPoint, sortedGrid: Map[(Int, Int), Array[DBScanLabeledODPoint]]): Seq[DBScanLabeledODPoint] = {
    if (target == null || target.od == null) return Seq.empty
    val tQStart = System.currentTimeMillis()
    val results = mutable.ArrayBuffer[DBScanLabeledODPoint]()
    val tgx = floor((target.startLon + 1e-11) / distanceEps).toInt
    val tgy = floor((target.startLat + 1e-11) / distanceEps).toInt

    for (dx <- -1 to 1; dy <- -1 to 1) {
      sortedGrid.get((tgx + dx, tgy + dy)).foreach { cellPoints =>
        val startTimeLimit = target.startTime - timeEps
        val endTimeLimit = target.startTime + timeEps

        var i = findFirstIndex(cellPoints, startTimeLimit)
        var break = false
        while (i < cellPoints.length && !break) {
          val cand = cellPoints(i)
          if (cand != null && cand.od != null) {
            if (cand.startTime > endTimeLimit) break = true
            else {
              if (target.isNeighbor(cand, distanceEps, timeEps)) results += cand
            }
          }
          i += 1
        }
      }
    }
    neighborSearchTime += (System.currentTimeMillis() - tQStart)
    results
  }

  private def findFirstIndex(arr: Array[DBScanLabeledODPoint], targetTime: Long): Int = {
    var low = 0; var high = arr.length
    while (low < high) {
      val mid = (low + high) / 2
      if (arr(mid) != null && arr(mid).startTime < targetTime) low = mid + 1 else high = mid
    }
    low
  }

  private def expandCluster(
                             seed: DBScanLabeledODPoint,
                             neighbors: Seq[DBScanLabeledODPoint],
                             clusterId: Int,
                             sortedGrid: Map[(Int, Int), Array[DBScanLabeledODPoint]]
                           ): Unit = {
    val tExpStart = System.currentTimeMillis()
    var queriesTimeInThisRound = 0L

    seed.flag = Flag.Core
    seed.cluster = clusterId

    val queue = mutable.Queue[DBScanLabeledODPoint]()

    for (pn <- neighbors if pn != null && pn.od != null) {
      if (pn.cluster == DBScanLabeledODPoint.Unknown) {
        pn.cluster = clusterId
      }
      if (!pn.visited) {
        markVisited(pn)
        queue.enqueue(pn)
      }
    }

    while (queue.nonEmpty) {
      val p = queue.dequeue()

      val tQStart = System.currentTimeMillis()
      val pNeighbors = findNeighbors(p, sortedGrid)
      queriesTimeInThisRound += (System.currentTimeMillis() - tQStart)

      if (pNeighbors.size >= minPoints) {
        p.flag = Flag.Core
        for (pn <- pNeighbors if pn != null && pn.od != null) {
          if (pn.cluster == DBScanLabeledODPoint.Unknown) {
            pn.cluster = clusterId
          }
          if (!pn.visited) {
            markVisited(pn)
            queue.enqueue(pn)
          }
        }
      } else {
        if (p.flag == Flag.NotFlagged) {
          p.flag = Flag.Border
        }
      }
    }

    val tExpEnd = System.currentTimeMillis()
    clusterExpansionTime += ((tExpEnd - tExpStart) - queriesTimeInThisRound)
  }
}
package org.example.DBScanOD

import scala.collection.mutable
import scala.math._

class DBScan_OD(distanceEps: Double, timeEps: Long, minPoints: Int) {

  var sortTime: Long = 0L
  var indexTime: Long = 0L
  var neighborSearchTime: Long = 0L
  var clusterExpansionTime: Long = 0L
  var indexSpaceKb: Double = 0.0

  def fit(points: Iterable[ODPoint]): Array[DBScanLabeledODPoint] = {
    sortTime = 0L
    indexTime = 0L
    neighborSearchTime = 0L
    clusterExpansionTime = 0L
    indexSpaceKb = 0.0

    val tSortStart = System.currentTimeMillis()
    val labeledPoints = points.map(new DBScanLabeledODPoint(_)).toArray
      .sortBy(p => (p.startTime, p.startLon, p.startLat, p.endTime, p.endLon, p.endLat))
    sortTime = System.currentTimeMillis() - tSortStart

    val tIndexStart = System.currentTimeMillis()
    val grid = mutable.Map[(Int, Int), mutable.ArrayBuffer[DBScanLabeledODPoint]]()
    labeledPoints.foreach { p =>
      val gx = floor((p.startLon + 1e-11) / distanceEps).toInt
      val gy = floor((p.startLat + 1e-11) / distanceEps).toInt
      grid.getOrElseUpdate((gx, gy), mutable.ArrayBuffer()) += p
    }
    val sortedGrid = grid.mapValues(_.toArray.sortBy(_.startTime)).toMap
    indexTime = System.currentTimeMillis() - tIndexStart

    val G = sortedGrid.size
    val N = labeledPoints.length
    indexSpaceKb = (G * 80.0 + N * 4.0) / 1024.0

    var clusterId = 0
    labeledPoints.foreach { p =>
      if (!p.visited) {
        p.visited = true
        val neighbors = findNeighbors(p, sortedGrid)
        if (neighbors.size >= minPoints) {
          clusterId += 1
          expandCluster(p, neighbors, clusterId, sortedGrid)
        } else {
          p.flag = Flag.Noise
        }
      }
    }
    labeledPoints
  }

  private def findNeighbors(target: DBScanLabeledODPoint, sortedGrid: Map[(Int, Int), Array[DBScanLabeledODPoint]]): Seq[DBScanLabeledODPoint] = {
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
          if (cand.startTime > endTimeLimit) break = true
          else {
            if (target.isNeighbor(cand, distanceEps, timeEps)) results += cand
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
      if (arr(mid).startTime < targetTime) low = mid + 1 else high = mid
    }
    low
  }

  private def expandCluster(seed: DBScanLabeledODPoint, neighbors: Seq[DBScanLabeledODPoint], clusterId: Int, sortedGrid: Map[(Int, Int), Array[DBScanLabeledODPoint]]): Unit = {
    val tExpStart = System.currentTimeMillis()
    var queriesTimeThisRound = 0L

    seed.flag = Flag.Core; seed.cluster = clusterId
    val queue = mutable.Queue(neighbors: _*)
    while (queue.nonEmpty) {
      val p = queue.dequeue()
      if (!p.visited) {
        p.visited = true; p.cluster = clusterId

        val tQStart = System.currentTimeMillis()
        val pNeighbors = findNeighbors(p, sortedGrid)
        queriesTimeThisRound += (System.currentTimeMillis() - tQStart)

        if (pNeighbors.size >= minPoints) {
          p.flag = Flag.Core; queue.enqueue(pNeighbors: _*)
        } else p.flag = Flag.Border
      } else if (p.cluster == DBScanLabeledODPoint.Unknown) {
        p.cluster = clusterId; p.flag = Flag.Border
      }
    }
    val tExpEnd = System.currentTimeMillis()
    clusterExpansionTime += ((tExpEnd - tExpStart) - queriesTimeThisRound)
  }
}
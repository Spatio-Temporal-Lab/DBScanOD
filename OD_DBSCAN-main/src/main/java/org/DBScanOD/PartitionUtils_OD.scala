package org.DBScanOD.spark

import org.apache.spark.rdd.RDD
import scala.collection.mutable

case class PartitionResult(
                            gridToPartition: Map[PartitionUtils_OD.CubeId, Int],
                            partitionMBRs: Map[Int, NonAnchorMBR],
                            deltaDist: Double,
                            deltaTime: Double
                          )

object PartitionUtils_OD {

  case class CubeId(gx: Int, gy: Int, gt: Int)

  def getPartitions(data: RDD[ODPoint],
                    partitionsNum: Int,
                    distanceEps: Double,
                    timeEps: Long,
                    cubeMultiplierDist: Double = 1.0,
                    cubeMultiplierTime: Double = 1.0,
                    alpha: Double = 0.5): PartitionResult = {

    val deltaDist = distanceEps * cubeMultiplierDist
    val deltaTime = timeEps.toDouble * cubeMultiplierTime

    val cubeData = data.filter(_ != null).flatMap { p =>
      val agx = math.floor((p.startLon + 1e-9) / deltaDist).toInt
      val agy = math.floor((p.startLat + 1e-9) / deltaDist).toInt
      val agt = math.floor((p.startTime + 1e-9) / deltaTime).toInt
      val anchorId = CubeId(agx, agy, agt)

      val ngx = math.floor((p.endLon + 1e-9) / deltaDist).toInt
      val ngy = math.floor((p.endLat + 1e-9) / deltaDist).toInt
      val ngt = math.floor((p.endTime + 1e-9) / deltaTime).toInt
      val nonAnchorId = CubeId(ngx, ngy, ngt)

      Seq((anchorId, (1L, nonAnchorId)))
    }.aggregateByKey((0L, mutable.Set[CubeId]()))(
      (acc, v) => {
        acc._2.add(v._2)
        (acc._1 + v._1, acc._2)
      },
      (acc1, acc2) => {
        acc1._2 ++= acc2._2
        (acc1._1 + acc2._1, acc1._2)
      }
    ).mapValues(v => (v._1, v._2.toSet)).collect()

    val n = cubeData.length
    if (n == 0) return PartitionResult(Map(), Map(), deltaDist, deltaTime)

    val cubes = cubeData.map(_._1)
    val counts = cubeData.map(_._2._1)
    val nonAnchorSets = cubeData.map(_._2._2)

    val adj = Array.fill(n)(mutable.ListBuffer[(Int, Double)]())
    val cubeToIndex = cubes.zipWithIndex.toMap

    for (i <- 0 until n) {
      val c1 = cubes(i)
      val count1 = counts(i).toDouble
      val set1 = nonAnchorSets(i)

      for (dx <- -1 to 1; dy <- -1 to 1; dt <- -1 to 1) {
        if (dx != 0 || dy != 0 || dt != 0) {
          val neighborId = CubeId(c1.gx + dx, c1.gy + dy, c1.gt + dt)
          cubeToIndex.get(neighborId).foreach { j =>
            val count2 = counts(j).toDouble
            val set2 = nonAnchorSets(j)

            val intersectSize = set1.intersect(set2).size
            val unionSize = set1.size + set2.size - intersectSize
            val sim = if (unionSize == 0) 0.0 else intersectSize.toDouble / unionSize

            val weight = math.sqrt(count1 * count2) * sim
            adj(i).append((j, weight))
          }
        }
      }
    }

    val partitionWeights = Array.fill(partitionsNum)(0L)
    val partitionAssignments = Array.fill(n)(0)
    val sortedIndices = counts.zipWithIndex.sortBy(-_._1).map(_._2)

    for (idx <- sortedIndices) {
      val minPart = partitionWeights.zipWithIndex.minBy(_._1)._2
      partitionAssignments(idx) = minPart
      partitionWeights(minPart) += counts(idx)
    }

    var improved = true
    var iterations = 0
    val maxIterations = 30

    while (improved && iterations < maxIterations) {
      improved = false
      iterations += 1
      val evalOrder = scala.util.Random.shuffle((0 until n).toList)

      for (u <- evalOrder) {
        val currentPart = partitionAssignments(u)
        val uWeight = counts(u)
        val neighborPartCosts = mutable.Map[Int, Double]().withDefaultValue(0.0)
        var internalCost = 0.0

        for ((v, edgeWeight) <- adj(u)) {
          val vPart = partitionAssignments(v)
          if (vPart != currentPart) neighborPartCosts(vPart) += edgeWeight
          else internalCost += edgeWeight
        }

        if (neighborPartCosts.nonEmpty) {
          var bestGain = 0.0
          var bestTargetPart = currentPart

          val currMax = partitionWeights.max
          val currMin = partitionWeights.min
          val currentPenalty = currMax - currMin

          var maxCount = 0
          var minCount = 0
          var i = 0
          while (i < partitionsNum) {
            val w = partitionWeights(i)
            if (w == currMax) maxCount += 1
            if (w == currMin) minCount += 1
            i += 1
          }

          val wCurr = partitionWeights(currentPart)

          for ((targetPart, targetCutCost) <- neighborPartCosts) {
            val cutGain = targetCutCost - internalCost
            val wTarget = partitionWeights(targetPart)

            val wCurrNew = wCurr - uWeight
            val wTargetNew = wTarget + uWeight

            val newMax = if (wTargetNew > currMax) wTargetNew
            else if (wCurr == currMax && maxCount == 1 && wCurrNew < currMax) partitionWeights.updated(currentPart, wCurrNew).max
            else currMax

            val newMin = if (wCurrNew < minCount && wCurrNew < currMin) wCurrNew
            else if (wTarget == currMin && minCount == 1 && wTargetNew > currMin) partitionWeights.updated(targetPart, wTargetNew).min
            else currMin

            val newPenalty = newMax - newMin
            val totalGain = cutGain + alpha * (currentPenalty - newPenalty).toDouble

            if (totalGain > bestGain) {
              bestGain = totalGain
              bestTargetPart = targetPart
            }
          }

          if (bestGain > 0.001) {
            partitionWeights(currentPart) -= uWeight
            partitionWeights(bestTargetPart) += uWeight
            partitionAssignments(u) = bestTargetPart
            improved = true
          }
        }
      }
    }

    val gridToPartition = mutable.Map[CubeId, Int]()
    for (i <- 0 until n) {
      gridToPartition(cubes(i)) = partitionAssignments(i)
    }
    val gridToPartitionMap = gridToPartition.toMap

    val gridToPartBroadcast = data.context.broadcast(gridToPartitionMap)
    val partitionMBRs = data.filter(_ != null).flatMap { point =>
      val gx = math.floor((point.startLon + 1e-9) / deltaDist).toInt
      val gy = math.floor((point.startLat + 1e-9) / deltaDist).toInt
      val gt = math.floor((point.startTime + 1e-9) / deltaTime).toInt
      gridToPartBroadcast.value.get(CubeId(gx, gy, gt)).map(pId => (pId, point))
    }.aggregateByKey(
      (Double.MaxValue, Double.MinValue, Double.MaxValue, Double.MinValue, Long.MaxValue, Long.MinValue)
    )(
      (acc, p) => (
        math.min(acc._1, p.endLon), math.max(acc._2, p.endLon),
        math.min(acc._3, p.endLat), math.max(acc._4, p.endLat),
        math.min(acc._5, p.endTime), math.max(acc._6, p.endTime)
      ),
      (acc1, acc2) => (
        math.min(acc1._1, acc2._1), math.max(acc1._2, acc2._2),
        math.min(acc1._3, acc2._3), math.max(acc1._4, acc2._4),
        math.min(acc1._5, acc2._5), math.max(acc1._6, acc2._6)
      )
    ).mapValues { case (minLon, maxLon, minLat, maxLat, minTime, maxTime) =>
      NonAnchorMBR(minLon, maxLon, minLat, maxLat, minTime, maxTime)
    }.collectAsMap().toMap

    PartitionResult(gridToPartitionMap, partitionMBRs, deltaDist, deltaTime)
  }
}
package org.example.DBScanOD

object DBScanOD_wrap {
  def train(data: Iterable[ODPoint],
            distanceEps: Double,
            timeEps: Long,
            minPoints: Int
           ): DBScanOD_wrap = {
    new DBScanOD_wrap(distanceEps, timeEps, minPoints, null, 0L, 0L, 0L, 0L, 0.0).train(data)
  }
}

class DBScanOD_wrap private(
                             val distanceEps: Double,
                             val timeEps: Long,
                             val minPoints: Int,
                             @transient private val labeledPointsAll: Array[DBScanLabeledODPoint],
                             val sortTime: Long,
                             val indexTime: Long,
                             val neighborSearchTime: Long,
                             val clusterExpansionTime: Long,
                             val indexSpaceKb: Double
                           ) extends Serializable {

  def labeledPoints: Array[DBScanLabeledODPoint] = labeledPointsAll

  private def train(data: Iterable[ODPoint]): DBScanOD_wrap = {
    println(s"[DBScanOD_wrap] OD records size: ${data.size}")

    val dbscan = new DBScan_OD(distanceEps, timeEps, minPoints)
    val clustered = dbscan.fit(data)

    val clusterSizes = clustered.groupBy(_.cluster).mapValues(_.size)
    clusterSizes.foreach { case (cluster, size) =>
      println(s"Cluster $cluster has $size points.")
    }

    println("[DBScanOD_wrap] Done.")
    new DBScanOD_wrap(
      distanceEps,
      timeEps,
      minPoints,
      clustered.toArray,
      dbscan.sortTime,
      dbscan.indexTime,
      dbscan.neighborSearchTime,
      dbscan.clusterExpansionTime,
      dbscan.indexSpaceKb
    )
  }
}
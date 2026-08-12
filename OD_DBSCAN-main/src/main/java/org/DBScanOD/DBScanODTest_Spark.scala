package org.DBScanOD.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import java.text.SimpleDateFormat

object DBScanODTest_Spark {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: DBScanODTest_Spark <input_path> <output_path> [distanceEps] [timeEps] [minPoints] [partitions] [cubeMultiplierDist] [cubeMultiplierTime] [alpha]")
      System.exit(1)
    }

    val inputFile          = args(0)
    val resultPath         = args(1)
    val distanceEps        = if (args.length > 2) args(2).toDouble else 0.05
    val timeEps            = if (args.length > 3) args(3).toLong else 1800
    val minPoints          = if (args.length > 4) args(4).toInt else 20
    val partitions         = if (args.length > 5) args(5).toInt else 16
    val cubeMultiplierDist = if (args.length > 6) args(6).toDouble else 1.0
    val cubeMultiplierTime = if (args.length > 7) args(7).toDouble else 1.0
    val alpha              = if (args.length > 8) args(8).toDouble else 0.5

    val conf = new SparkConf().setAppName("Distributed_DBScan_OD")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val shuffleListener = new SparkListener {
      var totalShuffleReadBytes: Long = 0L
      var totalShuffleWriteBytes: Long = 0L

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val metrics = taskEnd.taskMetrics
        if (metrics != null) {
          totalShuffleReadBytes += metrics.shuffleReadMetrics.totalBytesRead
          totalShuffleWriteBytes += metrics.shuffleWriteMetrics.bytesWritten
        }
      }
    }
    sc.addSparkListener(shuffleListener)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(s"\n[DBScanODTest_Spark] Started reading data from $inputFile ...")

    val fileReadStart = System.currentTimeMillis()
    val odRDD = sc.textFile(inputFile, partitions).flatMap { line =>
      try {
        val arr = line.split(",")
        if (arr.length >= 7) {
          val taxiId = arr(0).trim
          val startLon = arr(1).toDouble
          val startLat = arr(2).toDouble
          val startTime = dateFormat.parse(arr(3)).getTime / 1000
          val endLon = arr(4).toDouble
          val endLat = arr(5).toDouble
          val endTime = dateFormat.parse(arr(6)).getTime / 1000
          Some(ODPoint(taxiId, startLon, startLat, startTime, endLon, endLat, endTime))
        } else None
      } catch {
        case _: Exception => None
      }
    }

    odRDD.cache()
    val totalUniquePoints = odRDD.count()
    val fileReadEnd = System.currentTimeMillis()
    val dataLoadTime = fileReadEnd - fileReadStart

    val algoStartTime = System.currentTimeMillis()
    val dbscanRes = DBScanOD_Distributed.train(
      data = odRDD,
      distanceEps = distanceEps,
      timeEps = timeEps,
      minPoints = minPoints,
      numberOfPartitions = partitions,
      cubeMultiplierDist = cubeMultiplierDist,
      cubeMultiplierTime = cubeMultiplierTime,
      alpha = alpha
    )
    val labeledPointsRDD = dbscanRes.labeledPoints
    labeledPointsRDD.cache()

    val clusterSizes = labeledPointsRDD.map(p => (p.cluster, 1)).reduceByKey(_ + _).collect()
    val algoEndTime = System.currentTimeMillis()
    val algoTotalTime = algoEndTime - algoStartTime

    val writeStart = System.currentTimeMillis()
    val formattedResultRDD = labeledPointsRDD.map { p =>
      s"${p.taxiId},${p.cluster},${p.flag},${p.startLon},${p.startLat},${p.startTime},${p.endLon},${p.endLat},${p.endTime}"
    }
    formattedResultRDD.saveAsTextFile(resultPath)
    val writeEnd = System.currentTimeMillis()
    val diskWriteTime = writeEnd - writeStart

    val speedup = if (algoTotalTime > 0) dbscanRes.totalTaskTime.toDouble / algoTotalTime else 1.0
    val throughput = if (algoTotalTime > 0) totalUniquePoints.toDouble / (algoTotalTime / 1000.0) else 0.0

    println("\n" + "=" * 65)
    println("               DBSCAN OD 分布式时空实验指标测试报告")
    println("=" * 65)
    println(f"1. 数据加载与解析耗时     : $dataLoadTime%,d ms")
    println(f"2. 空间图划分与分区构建耗时: ${dbscanRes.partitionTime}%,d ms")
    println(f"3. 确定性排序平均耗时 (Avg): ${dbscanRes.sortTime}%,d ms")
    println(f"4. 索引构建平均耗时 (Avg)  : ${dbscanRes.indexTime}%,d ms")
    println(f"5. 邻域检索平均耗时 (Avg)  : ${dbscanRes.neighborSearchTime}%,d ms")
    println(f"6. 聚类扩张平均耗时 (Avg)  : ${dbscanRes.clusterExpansionTime}%,d ms")
    println(f"7. 结果磁盘写出耗时       : $diskWriteTime%,d ms")
    println(f"8. 算法总运行 Wall-clock 耗时: $algoTotalTime%,d ms")
    println(f"9. 索引物理空间开销 (Avg)  : ${dbscanRes.indexSpaceKb}%,.2f KB")
    println(f"10.处理吞吐量             : $throughput%,.2f points/sec")
    println(f"11.等效并行加速比         : $speedup%,.2f")
    println(f"12.分区数据倾斜度         : ${dbscanRes.partitionSkewness}%,.2f")
    println(f"13.计算时间倾斜度         : ${dbscanRes.taskSkewness}%,.2f")
    println(f"14.边界冗余数据复制率     : ${dbscanRes.replicationRatio}%,.2f")
    println(f"15.全局合并与冲突解决时间 : ${dbscanRes.globalMergeTime}%,d ms")
    println(f"16.分布式Shuffle总读/写   : Read: ${shuffleListener.totalShuffleReadBytes / 1024.0}%,.2f KB, Write: ${shuffleListener.totalShuffleWriteBytes / 1024.0}%,.2f KB")
    println("=" * 65 + "\n")

    println("================ 局部簇计数统计结果 ================")
    clusterSizes.sortBy(_._1).foreach { case (cluster, size) =>
      if (cluster == DBScanLabeledODPoint.Unknown || cluster == 0) {
        println(s"Noise/Unknown has $size points.")
      } else {
        println(s"Cluster $cluster has $size points.")
      }
    }
    println("==================================================\n")

    sc.stop()
  }
}
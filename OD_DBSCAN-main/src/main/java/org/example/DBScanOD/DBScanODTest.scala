package org.example.DBScanOD

import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat

object DBScanODTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: DBScanODTest <input_path> <output_path> [distanceEps] [timeEps] [minPoints] [partitions]")
      System.exit(1)
    }

    val inputFile   = args(0)
    val resultPath  = args(1)
    val distanceEps = if (args.length > 2) args(2).toDouble else 0.05
    val timeEps     = if (args.length > 3) args(3).toLong else 1800
    val minPoints   = if (args.length > 4) args(4).toInt else 20
    val partitions  = if (args.length > 5) args(5).toInt else 16

    val conf = new SparkConf().setAppName("SingleNode_DMI_DBScan_OD")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(s"\n[DBScanODTest] Started reading data from $inputFile ...")

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

    val odDataLocal = odRDD.collect()
    val totalPoints = odDataLocal.length
    val fileReadEnd = System.currentTimeMillis()
    val dataLoadTime = fileReadEnd - fileReadStart
    println(s"[DBScanODTest] Data successfully loaded to Driver memory. Total records = $totalPoints")


    val algoStartTime = System.currentTimeMillis()

    val dbscan = new DBScan_OD(distanceEps, timeEps, minPoints)
    val clustered = dbscan.fit(odDataLocal)

    val algoEndTime = System.currentTimeMillis()
    val algoTotalTime = algoEndTime - algoStartTime

    val writeStart = System.currentTimeMillis()
    val clusteredRDD = sc.parallelize(clustered, partitions)
    val formattedResultRDD = clusteredRDD.map { p =>
      s"${p.taxiId},${p.cluster},${p.flag},${p.startLon},${p.startLat},${p.startTime},${p.endLon},${p.endLat},${p.endTime}"
    }
    formattedResultRDD.saveAsTextFile(resultPath)
    val writeEnd = System.currentTimeMillis()
    val diskWriteTime = writeEnd - writeStart

    val throughput = if (algoTotalTime > 0) totalPoints.toDouble / (algoTotalTime / 1000.0) else 0.0

    println("\n" + "=" * 65)
    println("          DBSCAN OD 单机版（集群运行环境）指标测试报告")
    println("=" * 65)
    println(f"1. 数据加载与解析耗时 (HDFS -> Driver) : $dataLoadTime%,d ms")
    println(f"2. 确定性排序耗时                       : ${dbscan.sortTime}%,d ms")
    println(f"3. DMI 索引构建耗时                     : ${dbscan.indexTime}%,d ms")
    println(f"4. 邻域检索耗时                         : ${dbscan.neighborSearchTime}%,d ms")
    println(f"5. 聚类扩张耗时                         : ${dbscan.clusterExpansionTime}%,d ms")
    println(f"6. 结果 HDFS 磁盘写出耗时              : $diskWriteTime%,d ms")
    println(f"7. 算法单机运行总 Wall-clock 耗时       : $algoTotalTime%,d ms")
    println(f"8. DMI 索引物理空间开销                : ${dbscan.indexSpaceKb}%,.2f KB")
    println(f"9. 处理吞吐量                          : $throughput%,.2f points/sec")
    println("=" * 65 + "\n")

    val clusterSizes = clustered.groupBy(_.cluster).mapValues(_.size).toSeq.sortBy(_._1)
    println("================ 局部簇计数统计结果 ================")
    clusterSizes.foreach { case (cluster, size) =>
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
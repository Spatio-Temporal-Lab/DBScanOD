package org.example.DBScanOD

import scala.math._

case class ODPoint(
                    taxiId: String,
                    startLon: Double,
                    startLat: Double,
                    startTime: Long,
                    endLon: Double,
                    endLat: Double,
                    endTime: Long
                  )

object Flag extends Enumeration {
  type Flag = Value
  val Core, Border, Noise = Value
}

object DBScanLabeledODPoint {
  val Unknown = 0
}

class DBScanLabeledODPoint(od: ODPoint) extends ODPoint(
  od.taxiId, od.startLon, od.startLat, od.startTime,
  od.endLon, od.endLat, od.endTime
) {

  var cluster: Int = DBScanLabeledODPoint.Unknown
  var visited: Boolean = false
  var flag: Flag.Value = _
  var preprocessedNeighborCount: Int = 0
  var canBeCore: Boolean = true

  def isNeighbor(other: DBScanLabeledODPoint, distEps: Double, timeEps: Long): Boolean = {
    if (abs(this.startTime - other.startTime) > timeEps) return false
    if (abs(this.startLon - other.startLon) > distEps) return false
    if (abs(this.startLat - other.startLat) > distEps) return false

    if (abs(this.endTime - other.endTime) > timeEps) return false
    if (abs(this.endLon - other.endLon) > distEps) return false
    if (abs(this.endLat - other.endLat) > distEps) return false

    true
  }

  override def toString: String = {
    s"LabeledODPoint(taxiId=$taxiId, cluster=$cluster, flag=$flag)"
  }
}
package org.DBScanOD.spark

import scala.math.abs

case class ODPoint(
                    taxiId: String,
                    startLon: Double,
                    startLat: Double,
                    startTime: Long,
                    endLon: Double,
                    endLat: Double,
                    endTime: Long
                  )


case class NonAnchorMBR(
                         minLon: Double,
                         maxLon: Double,
                         minLat: Double,
                         maxLat: Double,
                         minTime: Long,
                         maxTime: Long
                       ) extends Serializable {

  def containsExpanded(endLon: Double, endLat: Double, endTime: Long, distEps: Double, timeEps: Long): Boolean = {
    endLon >= (minLon - distEps) && endLon <= (maxLon + distEps) &&
      endLat >= (minLat - distEps) && endLat <= (maxLat + distEps) &&
      endTime >= (minTime - timeEps) && endTime <= (maxTime + timeEps)
  }
}

object Flag extends Enumeration {
  type Flag = Value
  val Core, Border, Noise, NotFlagged = Value
}

object DBScanLabeledODPoint {
  val Unknown = 0
}

class DBScanLabeledODPoint(val od: ODPoint) extends Serializable {
  var cluster: Int = DBScanLabeledODPoint.Unknown
  var visited: Boolean = false
  var flag: Flag.Value = Flag.NotFlagged

  def taxiId: String = if (od != null) od.taxiId else ""
  def startLon: Double = if (od != null) od.startLon else 0.0
  def startLat: Double = if (od != null) od.startLat else 0.0
  def startTime: Long = if (od != null) od.startTime else 0L
  def endLon: Double = if (od != null) od.endLon else 0.0
  def endLat: Double = if (od != null) od.endLat else 0.0
  def endTime: Long = if (od != null) od.endTime else 0L

  def isNeighbor(other: DBScanLabeledODPoint, distEps: Double, timeEps: Long): Boolean = {
    if (this.od == null || other == null || other.od == null) return false
    if (abs(this.startTime - other.startTime) > timeEps) return false
    if (abs(this.startLon - other.startLon) > distEps) return false
    if (abs(this.startLat - other.startLat) > distEps) return false
    if (abs(this.endTime - other.endTime) > timeEps) return false
    if (abs(this.endLon - other.endLon) > distEps) return false
    if (abs(this.endLat - other.endLat) > distEps) return false
    true
  }

  override def toString: String = {
    s"LabeledODPoint(taxiId=${taxiId}, cluster=$cluster, flag=$flag)"
  }
}
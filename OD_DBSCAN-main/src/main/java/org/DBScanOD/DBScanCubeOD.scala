package org.DBScanOD.spark

case class DBScanCubeOD(
                         sx: Double, sy: Double, st: Double, ex: Double, ey: Double, et: Double,
                         sx2: Double, sy2: Double, st2: Double, ex2: Double, ey2: Double, et2: Double
                       ) extends Serializable {

  def contains(other: DBScanCubeOD): Boolean = {
    sx <= other.sx && other.sx2 <= sx2 &&
      sy <= other.sy && other.sy2 <= sy2 &&
      st <= other.st && other.st2 <= st2 &&
      ex <= other.ex && other.ex2 <= ex2 &&
      ey <= other.ey && other.ey2 <= ey2 &&
      et <= other.et && other.et2 <= et2
  }

  def contains(point: ODPoint): Boolean = {
    sx <= point.startLon && point.startLon <= sx2 &&
      sy <= point.startLat && point.startLat <= sy2 &&
      st <= point.startTime && point.startTime <= st2 &&
      ex <= point.endLon && point.endLon <= ex2 &&
      ey <= point.endLat && point.endLat <= ey2 &&
      et <= point.endTime && point.endTime <= et2
  }

  def almostContains(point: ODPoint): Boolean = {
    sx < point.startLon && point.startLon < sx2 &&
      sy < point.startLat && point.startLat < sy2 &&
      st < point.startTime && point.startTime < st2 &&
      ex < point.endLon && point.endLon < ex2 &&
      ey < point.endLat && point.endLat < ey2 &&
      et < point.endTime && point.endTime < et2
  }

  def shrink(distEps: Double, timeEps: Double): DBScanCubeOD = {
    DBScanCubeOD(
      sx + distEps, sy + distEps, st + timeEps, ex + distEps, ey + distEps, et + timeEps,
      sx2 - distEps, sy2 - distEps, st2 - timeEps, ex2 - distEps, ey2 - distEps, et2 - timeEps
    )
  }
}
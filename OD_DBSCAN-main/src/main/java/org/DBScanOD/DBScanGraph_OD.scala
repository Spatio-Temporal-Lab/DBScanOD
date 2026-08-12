package org.DBScanOD.spark

import scala.annotation.tailrec

object DBScanGraph_OD {
  def apply[T](): DBScanGraph_OD[T] = {
    new DBScanGraph_OD[T](Map[T, Set[T]]())
  }
}

class DBScanGraph_OD[T] private(nodes: Map[T, Set[T]]) extends Serializable {

  def addVertex(v: T): DBScanGraph_OD[T] = {
    nodes.get(v) match {
      case Some(_) => this
      case None => new DBScanGraph_OD(nodes + (v -> Set()))
    }
  }

  def insertEdge(from: T, to: T): DBScanGraph_OD[T] = {
    nodes.get(from) match {
      case None => new DBScanGraph_OD[T](nodes + (from -> Set(to)))
      case Some(edges) => new DBScanGraph_OD[T](nodes + (from -> (edges + to)))
    }
  }

  def connect(one: T, another: T): DBScanGraph_OD[T] = {
    insertEdge(one, another).insertEdge(another, one)
  }

  @tailrec
  private def getAdjacent(tovisit: Set[T], visited: Set[T], adjacent: Set[T]): Set[T] = {
    tovisit.headOption match {
      case Some(current) =>
        nodes.get(current) match {
          case Some(edges) =>
            getAdjacent(edges.diff(visited) ++ tovisit.tail, visited + current, adjacent ++ edges)
          case None => getAdjacent(tovisit.tail, visited, adjacent)
        }
      case None => adjacent
    }
  }

  def getConnected(from: T): Set[T] = {
    getAdjacent(Set(from), Set[T](), Set[T]()) - from
  }
}
package com.okg.message

import scala.collection.mutable

case class RoutingTable(map: mutable.Map[Int, Int]) extends Message {

  def get(key: Int) = {
    map.get(key).get
  }

  def contains(key: Int) = {
    map.contains(key)
  }

  def remove(key: Int) = {
    map -= key
  }

  def size() = {
    map.size
  }
}

object RoutingTable {
  def apply(map: mutable.Map[Int, Int]): RoutingTable = new RoutingTable(map)
}

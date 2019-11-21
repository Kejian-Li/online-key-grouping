package com.okg.message

import akka.dispatch.ControlMessage

import scala.collection.mutable

/**
  * Message inherited from {@Link ControlMessage} can be inserted into the head of mail-box's queue
  * @param map routing table
  */
case class RoutingTable(map: mutable.Map[Int, Int]) extends ControlMessage {

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

  def isEmpty() = {
    map.isEmpty
  }
}

object RoutingTable {
  def apply(map: mutable.Map[Int, Int]): RoutingTable = new RoutingTable(map)
}

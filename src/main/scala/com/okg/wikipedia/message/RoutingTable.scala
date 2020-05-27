package com.okg.wikipedia.message

import akka.dispatch.ControlMessage

import scala.collection.mutable

/**
  * Message inherited from {@Link ControlMessage} can be inserted into the head of mail-box's queue
  * @param map routing table
  */
case class RoutingTable(map: mutable.Map[String, Int]) extends ControlMessage {

  def get(key: String) = {
    map.get(key).get
  }

  def contains(key: String) = {
    map.contains(key)
  }

  def remove(key: String) = {
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
  def apply(map: mutable.Map[String, Int]): RoutingTable = new RoutingTable(map)
}

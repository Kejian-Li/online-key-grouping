package com.okg.wikipedia.message

import akka.dispatch.ControlMessage

import scala.collection.mutable

/**
  * Message inherited from {@Link ControlMessage} can be inserted into the head of mail-box's queue
  *
  * @param map migration table
  */
case class MigrationTable(map: mutable.Map[String, Pair]) extends ControlMessage {

  def contains(key: String) = {
    map.contains(key)
  }

  def put(key: String, pair: Pair) = {
    map.put(key, pair)
  }

  def size() = {
    map.size
  }
}

case class Pair(before: Option[Int], after: Option[Int])

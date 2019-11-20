package com.okg.message

import scala.collection.mutable

case class MigrationTable(map: mutable.Map[Int, Pair]) extends Message {

  def contains(key: Int)= {
    map.contains(key)
  }

  def put(key: Int, pair: Pair) = {
    map.put(key, pair)
  }

  def size() = {
    map.size
  }
}

case class Pair(before: Option[Int], after: Option[Int])

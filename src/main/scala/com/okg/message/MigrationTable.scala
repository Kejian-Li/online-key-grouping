package com.okg.message

import scala.collection.mutable

case class MigrationTable(map: mutable.Map[Int, Pair]) extends Message {

  def put(key: Int, pair: Pair): Unit = {
    map.put(key, pair)
  }
}

case class Pair(before: Int, after: Int)

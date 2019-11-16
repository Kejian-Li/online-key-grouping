package com.okg.message

import scala.collection.mutable

case class MigrationTable(map: mutable.Map[Int, Entry]) extends Message

case class Entry(key: Int, before: Int, after: Int)

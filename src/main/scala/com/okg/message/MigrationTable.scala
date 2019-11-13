package com.okg.message

case class MigrationTable(list: List[Entry]) extends Message

object MigrationTable{
  def apply(list: List[Entry]): MigrationTable = new MigrationTable(list)
}

case class Entry(key: Int, before: Int, after: Int)

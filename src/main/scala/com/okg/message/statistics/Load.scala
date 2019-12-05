package com.okg.message.statistics

/**
  * Used by SchedulerActor
  * @param index index of Operator instance
  * @param x  number of received tuples
  */
case class Load(index: Int, tuplesNum: Int)

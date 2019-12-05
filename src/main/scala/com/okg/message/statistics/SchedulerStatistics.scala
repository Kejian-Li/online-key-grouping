package com.okg.message.statistics

/**
  * Used by SchedulerActor to send statistics
  */
case class SchedulerStatistics(index: Int,
                               totalPeriod: Int,
                               averageDelayTime: Long)   // LEARN + WAIT

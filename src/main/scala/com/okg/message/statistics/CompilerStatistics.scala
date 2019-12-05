package com.okg.message.statistics

/**
  * Used by CompilerActor to send statistics of each COMPILE result
  * @param period
  * @param routingTableGenerationTime
  * @param routingTableSize
  * @param migrationTableSize
  */
case class CompilerStatistics(period: Int,
                         routingTableGenerationTime: Long,    // Microsecond
                         routingTableSize: Int,
                         migrationTableSize: Int)
package com.okg.message.statistics

import scala.concurrent.duration.Duration

/**
  * Used by CompilerActor to send statistics of each COMPILE result
  * @param period
  * @param routingTableGenerationTime
  * @param routingTableSize
  * @param migrationTableSize
  */
case class CompilerStatistics(period: Int,
                         routingTableGenerationTime: Duration,
                         routingTableSize: Int,
                         migrationTableSize: Int)
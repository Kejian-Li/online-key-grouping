package com.okg.message.statistics

/**
  * Message Used by InstanceActor to send statistics of each period
  * @param index index of instance
  * @param period current period
  * @param totalTupleNums number of tuples received so far
  */
case class InstanceStatistics(index: Int,
                              period: Int,
                              periodTuplesNum: Int,
                              totalTuplesNum: Int)

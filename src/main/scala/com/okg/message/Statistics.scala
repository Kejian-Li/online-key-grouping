package com.okg.message

/**
  * Message Used by  InstanceActor to send statistics each period
  * @param index index of instance
  * @param period current period
  * @param tupleNums number of tuples received so far
  */
case class Statistics(index: Int, period: Int, tupleNums: Int)

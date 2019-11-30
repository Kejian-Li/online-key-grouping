package com.okg.state

import com.okg.message.{RoutingTable, Sketch}

/**
  *
  * @param routingTable
  * @param sketches
  * @param migrationCompletedNotifications  number of completed migration notifications Operator instances send
  */
case class CompilerStateData(currentRoutingTable: RoutingTable,
                             sketches: List[Sketch],
                             var migrationCompletedNotifications: Int,
                             historicalBuckets: Array[Int])

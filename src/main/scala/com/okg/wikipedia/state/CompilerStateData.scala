package com.okg.wikipedia.state

import com.okg.wikipedia.message.{RoutingTable, Sketch}

/**
  *
  * @param routingTable
  * @param sketches
  * @param migrationCompletedNotifications  number of completed migration notifications Operator instances send
  */
case class CompilerStateData(currentRoutingTable: RoutingTable,
                             sketches: List[Sketch],
                             var migrationCompletedNotifications: Int,
                             historicalBuckets: Array[Int])     // data structure

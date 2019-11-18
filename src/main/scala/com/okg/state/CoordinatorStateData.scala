package com.okg.state

import com.okg.message.{RoutingTable, Sketch}

/**
  *
  * @param routingTable
  * @param sketches
  * @param notifications  number of notifications Operator instances send
  */
case class CoordinatorStateData(currentRoutingTable: RoutingTable,
                                sketches: List[Sketch],
                                notifications: Int)

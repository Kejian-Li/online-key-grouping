package com.okg.state

import com.okg.message.{RoutingTable, Sketch}
import com.okg.tuple.TupleQueue

/**
  *
  * @param tupleQueue
  * @param routingTable
  * @param sketches
  * @param notifications  number of notifications Operator instances send
  */
case class CoordinatorStateData(tupleQueue: TupleQueue[Int],
                                currentRoutingTable: RoutingTable,
                                sketches: List[Sketch],
                                notifications: Int)

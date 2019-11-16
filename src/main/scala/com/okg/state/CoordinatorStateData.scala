package com.okg.state

import com.okg.message.{RoutingTable, Sketch}
import com.okg.tuple.TupleQueue

case class CoordinatorStateData(tupleQueue: TupleQueue[Int],
                                routingTable: RoutingTable,
                                sketches: Seq[Sketch],
                                notifications: Int)

package com.okg.state

import com.okg.message.{RoutingTable, Sketch}
import com.okg.tuple.{Tuple, TupleQueue}
import com.okg.util.SpaceSaving

case class SchedulerStateData(N: Int,
                              m: Int,
                              var n: Int,
                              k: Int,
                              spaceSaving: SpaceSaving,
                              sketch: Sketch,
                              routingTable: RoutingTable,
                              tupleQueue: TupleQueue[Tuple[Int]])


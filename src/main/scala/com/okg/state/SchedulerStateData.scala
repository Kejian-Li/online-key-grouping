package com.okg.state

import com.okg.message.{RoutingTable, Sketch}
import com.okg.tuple.{Tuple, TupleQueue}
import com.okg.util.SpaceSaving

case class SchedulerStateData(m: Int,
                              k: Int,
                              spaceSaving: SpaceSaving,    // first data structure
                              buckets: Array[Int],         // second data structure
                              routingTable: RoutingTable,
                              tupleQueue: TupleQueue[Tuple[Int]])


package com.okg.wikipedia.state

import com.okg.wikipedia.message.RoutingTable
import com.okg.wikipedia.tuple.{Tuple, TupleQueue}
import com.okg.wikipedia.util.SpaceSaving

case class SchedulerStateData(m: Int,
                              k: Int,
                              spaceSaving: SpaceSaving[String],    // first data structure
                              buckets: Array[Int],         // second data structure
                              routingTable: RoutingTable,
                              tupleQueue: TupleQueue[Tuple[String]])


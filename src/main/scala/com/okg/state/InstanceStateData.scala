package com.okg.state

import com.okg.tuple.{Tuple, TupleQueue}

case class InstanceStateData(num: Int, tupleQueue: TupleQueue[Tuple[Int]])

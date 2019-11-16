package com.okg.state

import com.okg.tuple.{Tuple, TupleQueue}

case class InstanceStateData(tupleNums: Int, tupleQueue: TupleQueue[Tuple[Int]])

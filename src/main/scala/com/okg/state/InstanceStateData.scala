package com.okg.state

import scala.collection.mutable

/**
  *
  * @param tupleNums number of received tuples in total
  * @param tupleMap  map form received keys to their frequencies
  */
case class InstanceStateData(tupleNums: Int, tupleMap: mutable.Map[Int, Int])

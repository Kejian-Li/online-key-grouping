package com.okg.wikipedia.state

import com.okg.wikipedia.message.MigrationTable

import scala.collection.mutable

/**
  *
  * @param tuplesNum Number of received tuples in total
  * @param tupleMap  Map form received keys to their frequencies, it represents states of keys and it is virtual.
  *                  As different operators own different states. So we don't actually migrate this map
  *                  between instances of the operator. We just simply suppose that migration is completed and
  *                  schedulers use new routing table to assign tuples in the next period.
  */
case class InstanceStateData(period: Int,
                             tuplesNum: Int,
                             tupleMap: mutable.Map[String, Int],
                             migrationTable: MigrationTable)

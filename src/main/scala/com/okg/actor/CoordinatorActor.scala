package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import akka.japi.Option.Some
import com.okg.message._
import com.okg.state._

import scala.collection.mutable

/**
  * Class for Coordinator
  *
  * @param schedulerActors
  * @param instanceActors
  * @param s
  * @param k
  */
case class CoordinatorActor(s: Int, // number of Scheduler instances
                            instanceActors: Array[ActorRef])
  extends Actor with FSM[CoordinatorState, CoordinatorStateData] {

  val k = instanceActors.size
  val schedulerActorsSet = mutable.Set.empty[ActorRef]
  var nextRoutingTable = new RoutingTable(mutable.Map.empty[Int, Int]) // empty routing table

  val coordinatorStateData = new CoordinatorStateData(
    new RoutingTable(mutable.Map.empty[Int, Int]),
    List.empty[Sketch],
    0)

  startWith(WAIT_ALL, coordinatorStateData)

  when(WAIT_ALL) {
    case Event(sketch: Sketch, coordinatorStateData: CoordinatorStateData) => {

      log.info("Coordinator received sketch from " + sender())

      val newStateData = coordinatorStateData.copy(sketches = coordinatorStateData.sketches :+ sketch)
      log.info("Coordinator received sketch " + newStateData.sketches.size + " in total")
      if (newStateData.sketches.size == s) {
        log.info("Coordinator is gonna GENERATION state")
        goto(GENERATION) using (newStateData)
      } else {
        stay() using (newStateData)
      }
    }
  }

  when(GENERATION) {
    case Event(MigrationCompleted, coordinatorStateData: CoordinatorStateData) => {
      coordinatorStateData.migrationCompletedNotifications += 1

      if (coordinatorStateData.migrationCompletedNotifications == k) {
        if (nextRoutingTable.isEmpty) { // sanity check
          log.error("Coordinator: next routing table is empty")
        } else {
          log.info("Coordinator: new routing table size is " + nextRoutingTable.size())
        }
        goto(WAIT_ALL) using (
          new CoordinatorStateData(nextRoutingTable, List.empty[Sketch], 0))
      } else {
        stay()
      }
    }
  }

  whenUnhandled {
    case Event(StartSimulation, coordinatorStateData: CoordinatorStateData) => {
      schedulerActorsSet.add(sender())

      if (schedulerActorsSet.size == s) {
        for (i <- 0 to k - 1) {
          log.info("Coordinator: register myself at the instance " + i + " of the Operator")
          instanceActors(i) ! CoordinatorRegistration
        }
      }
      stay()
    }
  }

  onTransition {
    case WAIT_ALL -> GENERATION => {
      nextRoutingTable = generateRoutingTable(nextStateData)
      val migrationTable = makeMigrationTable(nextRoutingTable)
      instanceActors.foreach(instanceActor => {
        instanceActor ! new StartMigration(instanceActors, migrationTable)
      })
    }
    case GENERATION -> WAIT_ALL => {
      schedulerActorsSet.foreach(schedulerActor => {
        schedulerActor ! new StartAssignment(nextRoutingTable)
      })
    }
  }

  def findLeastLoad(buckets: Array[Int]): Int = {
    var minLoad = Int.MaxValue
    var minIndex = -1
    for (i <- 0 to buckets.length - 1) {
      if (buckets(i) < minLoad) {
        minLoad = buckets(i)
        minIndex = i
      }
    }
    minIndex
  }

  // build global mapping and return routing table
  def buildGlobalMappingFunction(heavyHitters: Seq[(Int, Int)],    //  (key, frequency)
                                 buckets: Array[Int]) = {
    val heavyHittersMapping = mutable.Map.empty[Int, Int]
    heavyHitters.foreach(
      entry => {
        val key = entry._1
        val leastIndex = findLeastLoad(buckets)
        buckets.update(leastIndex, buckets(leastIndex) + entry._2)
        heavyHittersMapping.put(key, leastIndex)
      }
    )
    log.info("Coordinator: build global mapping function successfully")
    log.info("Coordinator: next routing table size is: " + heavyHittersMapping.size)

    for (i <- 0 to buckets.length - 1) {
      log.info("Coordinator: final bucket " + i + " owns " + buckets(i) + " tuples")
    }
    new RoutingTable(heavyHittersMapping)
  }

  // generate a new routing table
  def generateRoutingTable(coordinatorStateData: CoordinatorStateData): RoutingTable = {
    val cumulativeHeavyHittersMap = mutable.TreeMap.empty[Int, Int]
    val cumulativeBuckets = new Array[Int](k)

    coordinatorStateData.sketches.foreach(
      sketch => {

        sketch.heavyHitters.foreach(
          entry => {
            val lastValue = cumulativeHeavyHittersMap.getOrElse(entry._1, 0)
            cumulativeHeavyHittersMap.update(entry._1, lastValue + entry._2)
          }
        )

        for (i <- 0 to k - 1) {
          cumulativeBuckets(i) += sketch.buckets(i)
        }
      }
    )

    for (i <- 0 to k - 1) {
      log.info("Coordinator: cumulative bucket " + i + " owns " + cumulativeBuckets(i) + " tuples")
    }

    val descendingHeavyHittersMap = cumulativeHeavyHittersMap.toSeq.sortWith(_._2 > _._2)

    log.info("Coordinator: " + " cumulative heavy hitters with their frequencies as follows in the descending order: ")
    descendingHeavyHittersMap.foreach{
      entry => {
        log.info(entry._1 + "  " + entry._2)
      }
    }

    buildGlobalMappingFunction(descendingHeavyHittersMap, cumulativeBuckets)
  }

  // compare currentRoutingTable with nextRoutingTable to make migration table
  def makeMigrationTable(nextRoutingTable: RoutingTable) = {

    val migrationTable = new MigrationTable(mutable.Map.empty[Int, Pair])
    val currentRoutingTable = nextStateData.currentRoutingTable

    currentRoutingTable.map.foreach {
      entry => {
        val key = entry._1
        if (nextRoutingTable.contains(key)) { // 1. both contain
          val after = nextRoutingTable.get(key)
          val pair = new Pair(Some(entry._2), Some(after))

          migrationTable.put(key, pair)
        } else { // 2. the old contains but the new doesn't
          migrationTable.put(key, new Pair(Some(entry._2), None))
        }
      }
    }

    nextRoutingTable.map.foreach {
      entry => {
        val key = entry._1
        if (!currentRoutingTable.contains(key)) { // 3. the new contains but the old doesn't
          migrationTable.put(key, new Pair(None, Some(entry._2)))
        }
      }
    }

    log.info("Coordinator: make next migration table successfully, its size is: " + migrationTable.size)
    migrationTable
  }
}

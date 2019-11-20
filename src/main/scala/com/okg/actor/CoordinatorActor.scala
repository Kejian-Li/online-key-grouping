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
case class CoordinatorActor(instanceActors: Array[ActorRef],
                            s: Int, // number of scheduler instances
                            k: Int) // number of operator instances
  extends Actor with FSM[CoordinatorState, CoordinatorStateData] {

  val schedulerActorsSet = Set.empty[ActorRef]
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
      coordinatorStateData.notifications += 1

      if (coordinatorStateData.notifications == s) {
        if (nextRoutingTable.map.isEmpty) { // sanity check
          log.error("Coordinator: next routing table is empty")
        }else {
          log.info("Coordinator: new routing table size is " + coordinatorStateData.currentRoutingTable.size())
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
      schedulerActorsSet.+(sender())
      for (i <- 0 to instanceActors.size) {
        instanceActors(i) ! CoordinatorRegistration
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
    for (i <- 0 to s - 1) {
      if (buckets(i) < minLoad) {
        minLoad = buckets(i)
        minIndex = i
      }
    }
    minIndex
  }

  // build global mapping and return routing table
  def buildGlobalMappingFunction(heavyHitters: Map[Int, Int],
                                 buckets: Array[Int]) = {
    val heavyHittersMapping = mutable.Map.empty[Int, Int]
    heavyHitters.foreach(
      entry => {
        val index = findLeastLoad(buckets)
        buckets.update(index, buckets(index) + heavyHitters.get(entry._1).get)
        heavyHittersMapping.put(entry._1, index)
      }
    )
    log.info("Coordinator: build global mapping function successfully")
    log.info("Coordinator: next routing table size is: " + heavyHittersMapping.size)
    new RoutingTable(heavyHittersMapping)
  }

  // generate a new routing table
  def generateRoutingTable(coordinatorStateData: CoordinatorStateData): RoutingTable = {
    val heavyHittersMap = mutable.TreeMap.empty[Int, Int]
    val buckets = new Array[Int](s)
    coordinatorStateData.sketches.foreach(
      sketch => {

        sketch.map.foreach(
          entry => {
            heavyHittersMap.put(entry._1, heavyHittersMap.getOrElse(entry._1, 0) + entry._2)
          }
        )

        for (i <- 0 to s - 1) {
          buckets(i) += sketch.A(i)
        }
      }
    )

    val descendingHeavyHittersMap = heavyHittersMap.toSeq.sortWith(_._2 > _._2).toMap

    buildGlobalMappingFunction(descendingHeavyHittersMap, buckets)
  }

  // compare currentRoutingTable with nextRoutingTable to make migration table
  def makeMigrationTable(nextRoutingTable: RoutingTable) = {

    val migrationTable = new MigrationTable(mutable.Map.empty[Int, Pair])
    val currentRoutingTable = nextStateData.currentRoutingTable

    currentRoutingTable.map.foreach {
      entry => {
        val key = entry._1
        if (nextRoutingTable.contains(key)) {     // 1. both contain
          val after = nextRoutingTable.get(key)
          val pair = new Pair(Some(entry._2), Some(after))

          migrationTable.put(key, pair)
        } else {                                  // 2. the old contains but the new doesn't
          migrationTable.put(key, new Pair(Some(entry._2), None))
        }
      }
    }

    nextRoutingTable.map.foreach {
      entry => {
        val key = entry._1
        if (!currentRoutingTable.contains(key)) {   // 3. the new contains but the old doesn't
          migrationTable.put(key, new Pair(None, Some(entry._2)))
        }
      }
    }

    log.info("Coordinator: make next migration table successfully, its size is: " + migrationTable.map.size)
    migrationTable
  }
}

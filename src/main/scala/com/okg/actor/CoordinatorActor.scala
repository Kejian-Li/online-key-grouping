package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
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

  val schedulerActors = Set.empty[ActorRef]
  var nextRoutingTable = new RoutingTable(mutable.Map.empty[Int, Int]) // empty routing table

  val coordinatorStateData = new CoordinatorStateData(
    new RoutingTable(mutable.Map.empty[Int, Int]),
    List.empty[Sketch],
    0)

  startWith(WAIT_ALL, coordinatorStateData)

  when(WAIT_ALL) {
    case Event(sketch: Sketch, coordinatorStateData: CoordinatorStateData) => {
      schedulerActors.+(sender())
      log.info("received sketch from " + sender())

      val newStateData = coordinatorStateData.copy(sketches = coordinatorStateData.sketches :+ sketch)
      log.info("received sketch " + newStateData.sketches.size + " in total")
      if (newStateData.sketches.size == s) {
        log.info("gonna GENERATION state")
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
          log.error("next routing table is empty")
        }
        goto(WAIT_ALL) using (
          new CoordinatorStateData(nextRoutingTable, List.empty[Sketch], 0))
      } else {
        stay()
      }
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
      schedulerActors.foreach(schedulerActor => {
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

  // build mapping and return routing table
  def buildGlobalMappingFunction(heavyHitters: mutable.Map[Int, Int],
                                 buckets: Array[Int]) = {
    val heavyHittersMapping = mutable.Map.empty[Int, Int]
    heavyHitters.foreach(
      entry => {
        val index = findLeastLoad(buckets)
        buckets.update(index, buckets(index) + heavyHitters.get(entry._1).get)
        heavyHittersMapping.put(entry._1, index)
      }
    )
    log.info("build global mapping function successfully")
    new RoutingTable(heavyHittersMapping)
  }

  // generate a new routing table
  def generateRoutingTable(coordinatorStateData: CoordinatorStateData): RoutingTable = {
    val heavyHitters = mutable.TreeMap.empty[Int, Int]
    val buckets = new Array[Int](s)
    coordinatorStateData.sketches.foreach(
      sketch => {

        sketch.map.foreach(
          entry => {
            heavyHitters.update(entry._1, heavyHitters.getOrElse(entry._1, 0) + entry._2)
          }
        )

        for (i <- 0 to s - 1) {
          buckets(i) += sketch.A(i)
        }
      }
    )

    buildGlobalMappingFunction(heavyHitters, buckets)
  }

  // compare currentRoutingTable with nextRoutingTable to make migration table
  def makeMigrationTable(nextRoutingTable: RoutingTable) = {
    val migrationTable = new MigrationTable(mutable.Map.empty[Int, Pair])
    val currentRoutingTable = nextStateData.currentRoutingTable
    currentRoutingTable.map.foreach {
      entry => {
        if (nextRoutingTable.containsKey(entry._1)) {
          val pair = new Pair(entry._2, nextRoutingTable.get(entry._1))
          migrationTable.put(entry._1, pair)
          nextRoutingTable.remove(entry._1)
        } else {
          migrationTable.put(entry._1, null)
        }
      }
    }
    nextRoutingTable.map.foreach {
      entry => {
        migrationTable.put(entry._1, new Pair(null, entry._2))
      }
    }
    migrationTable
  }
}

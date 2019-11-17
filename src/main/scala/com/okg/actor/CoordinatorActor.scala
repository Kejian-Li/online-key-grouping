package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.state._
import com.okg.tuple.TupleQueue

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

  val coordinatorStateData = new CoordinatorStateData(new TupleQueue[Int],
    new RoutingTable(mutable.Map.empty[Int, Int]),
    List.empty[Sketch],
    0)

  startWith(WAIT_ALL, coordinatorStateData)

  when(WAIT_ALL) {
    case Event(sketch: Sketch, coordinatorStateData: CoordinatorStateData) => {
      schedulerActors.incl(sender())
      coordinatorStateData.sketches :+ (sketch)

      log.info("sketch size is: " + coordinatorStateData.sketches.size)

      if (coordinatorStateData.sketches.size == s) {
        goto(GENERATION) using (coordinatorStateData.copy(sketches = List.empty[Sketch]))
      }
      stay()
    }
  }

  when(GENERATION) {
    case Event(MigrationCompleted, coordinatorStateData: CoordinatorStateData) => {
      coordinatorStateData.copy(notifications = coordinatorStateData.notifications + 1)
      if (coordinatorStateData.notifications == s) {
        if (nextRoutingTable.map.isEmpty) { // sanity check
          log.error("next routing table is empty")
        }
        goto(WAIT_ALL) using (coordinatorStateData.copy(notifications = 0, currentRoutingTable = nextRoutingTable))
      }
      stay()
    }
  }

  onTransition {
    case _ -> GENERATION => {
      nextRoutingTable = generateRoutingTable(nextStateData)
      val migrationTable = makeMigrationTable(nextRoutingTable)
      schedulerActors.foreach(schedulerActor => {
        schedulerActor ! migrationTable
      })
    }
    case _ -> WAIT_ALL => {
      schedulerActors.foreach(schedulerActor => {
        schedulerActor ! new StartAssignment(nextRoutingTable)
      })
    }
  }

  def findLeastLoad(buckets: Array[Int]): Int = {
    var minLoad = Int.MaxValue
    var minIndex = -1
    for (i <- 0 to s - 1) {
      if(buckets(i) < minLoad) {
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
            heavyHitters.put(entry._1, heavyHitters.getOrElse(entry._1, 0) + entry._2)
          }
        )

        for (i <- 0 to s) {
          buckets(i) += sketch.A(i)
        }
      }
    )

    buildGlobalMappingFunction(heavyHitters, buckets)
  }

  // compare currentRoutingTable with nextRoutingTable to make migration table
  def makeMigrationTable(nextRoutingTable: RoutingTable) = {

    new MigrationTable(mutable.Map.empty[Int, Entry])
  }
}
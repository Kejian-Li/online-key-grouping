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
  var nextRoutingTable = new RoutingTable(mutable.Map.empty[Int, Int])  // empty routing table

  val coordinatorStateData = new CoordinatorStateData(new TupleQueue[Int],
    new RoutingTable(mutable.Map.empty[Int, Int]),
    new Array[Sketch](s),
    0)

  startWith(WAIT_ALL, coordinatorStateData)

  when(WAIT_ALL) {
    case Event(sketch: Sketch, coordinatorStateData: CoordinatorStateData) => {
      schedulerActors.incl(sender())
      coordinatorStateData.sketches

      if (coordinatorStateData.sketches.size == s) {
        goto(GENERATION) using (coordinatorStateData.copy(sketches = new Array[Sketch](s)))
      }
      stay()
    }
  }

  when(GENERATION) {
    case Event(MigrationCompleted, coordinatorStateData: CoordinatorStateData) => {
      coordinatorStateData.copy(notifications = coordinatorStateData.notifications + 1)
      if (coordinatorStateData.notifications == s) {
        if(nextRoutingTable.map.isEmpty) {   // sanity check
          log.error("next routing table is empty")
        }
        goto(WAIT_ALL) using (coordinatorStateData.copy(notifications = 0, currentRoutingTable = nextRoutingTable))
      }
      stay()
    }
  }

  onTransition {
    case _ -> GENERATION => {
      nextRoutingTable = generateRoutingTable()
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

  def generateRoutingTable(): RoutingTable = {
    // generate a new routing table
    new RoutingTable(mutable.Map.empty[Int, Int])
  }

  def makeMigrationTable(nextRoutingTable: RoutingTable) = {

    // compare currentRoutingTable with nextRoutingTable to make migration table

    new MigrationTable(mutable.Map.empty[Int, Entry])
  }
}

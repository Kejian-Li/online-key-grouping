package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.state._
import com.okg.tuple.TupleQueue

import scala.collection.mutable

/**
  * Class for Coordinator
  * @param schedulerActors
  * @param instanceActors
  * @param s
  * @param k
  */
class CoordinatorActor(instanceActors: Array[ActorRef],
                       s: Int, // number of scheduler instances
                       k: Int)  // number of operator instances
  extends Actor with FSM[CoordinatorState, CoordinatorStateData] {

  val schedulerActors = Set.empty[ActorRef]

  val coordinatorStateData = new CoordinatorStateData(new TupleQueue[Int],
    new RoutingTable(mutable.Map.empty[Int, Int]),
    new Array[Sketch](s),
    0)

  startWith(WAIT_ALL, coordinatorStateData)

  when(WAIT_ALL) {
    case Event(sketch: Sketch, coordinatorStateData: CoordinatorStateData) => {
      schedulerActors.incl(sender())
      coordinatorStateData.sketches :+ sketch

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
        coordinatorStateData.copy(notifications = 0)
        goto(WAIT_ALL)
      }
      stay()
    }
  }


  onTransition {
    case _ -> GENERATION => {
      val routingTable = generateRoutingTable()
      val migrationTable = makeMigrationTable(routingTable)
      schedulerActors.foreach(schedulerActor => {
        schedulerActor ! migrationTable
      })
    }
  }

  def generateRoutingTable(): RoutingTable = {
    new RoutingTable()
  }

  def makeMigrationTable(routingTable: RoutingTable): mutable.HashMap[Int, Int] = {

    new MigrationTable()
  }
}

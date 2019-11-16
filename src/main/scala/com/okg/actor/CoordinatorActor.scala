package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.state._
import com.okg.tuple.TupleQueue

import scala.collection.mutable

class CoordinatorActor(schedulerActors: Array[ActorRef],
                       instanceActors: Array[InstanceActor],
                       k: Int,
                       l: Int)
  extends Actor with FSM[CoordinatorState, CoordinatorStateData] {

  startWith(WAIT_ALL, new CoordinatorStateData(new TupleQueue[Int], new RoutingTable()))

  when(WAIT_ALL) {
    case Event(sketch: Sketch, coordinatorStateData: CoordinatorStateData) => {
      if (coordinatorStateData.sketches.size == k) {
        goto(GENERATION) using (coordinatorStateData.copy(sketches = new Array[Sketch](k)))
      }
      stay() using (coordinatorStateData.copy(sketches = coordinatorStateData.sketches :+ sketch))
    }
  }

  when(GENERATION) {
    case Event(MigrationCompleted, coordinatorStateData: CoordinatorStateData) => {
      coordinatorStateData.copy(notifications = coordinatorStateData.notifications + 1)
      if(coordinatorStateData.notifications == k) {
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

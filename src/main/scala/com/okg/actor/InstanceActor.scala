package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.state._
import com.okg.tuple.Tuple

import scala.collection.mutable

/**
  * Class for Operator instance
  */
class InstanceActor(index: Int) extends Actor with FSM[InstanceState, InstanceStateData] {

  var coordinatorActorRef = Actor.noSender
  var instanceActors = Array.empty[ActorRef]

  startWith(RUN, new InstanceStateData(0, mutable.Map.empty[Int, Int]))

  when(RUN) {
    case Event(tuple: Tuple[Int], data: InstanceStateData) => {
      // enqueue tuple
      val key = tuple.key
      data.tupleMap.update(key, data.tupleMap.getOrElse(key, 0) + 1) // virtual, no real meaning
      stay() using (data.copy(tupleNums = data.tupleNums + 1))
    }

    case Event(startMigration: StartMigration, data: InstanceStateData) => {
      instanceActors = startMigration.instanceActors
      goto(MIGRATION)
    }
  }

  when(MIGRATION) {
    case Event(Done, data: InstanceStateData) => {
      goto(RUN)
    }
  }

  val schedulerActorsSet = Set.empty[ActorRef]
  var receivedTerminationNotification = 0
  whenUnhandled {
    case Event(StartSimulation, data: InstanceStateData) => {
      schedulerActorsSet.+(sender())
      stay()
    }
    case Event(CoordinatorRegistration, data: InstanceStateData) => {
      coordinatorActorRef = sender()
      stay()
    }

    case Event(TerminateSimulation, data: InstanceStateData) => {
      receivedTerminationNotification += 1
      if (receivedTerminationNotification == schedulerActorsSet.size) {
        sender() ! new Load(index, data.tupleNums) // tell simulation actor statistics
      }
      stay()
    }
  }

  onTransition {
    case RUN -> MIGRATION => {
      // migration procedure. it is omitted. we simply suppose that migration is completed
      self ! Done
    }
    case MIGRATION -> RUN => {
      coordinatorActorRef ! MigrationCompleted
    }
  }
}

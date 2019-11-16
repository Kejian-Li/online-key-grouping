package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message.{Done, MigrationCompleted, MigrationTable}
import com.okg.state._
import com.okg.tuple.{Tuple, TupleQueue}

/**
  * Class for Operator instance
  * @param instanceActors
  */
class InstanceActor(instanceActors: Array[ActorRef]) extends Actor with FSM[InstanceState, InstanceStateData] {

  var coordinatorRef = Actor.noSender
  startWith(RUN, new InstanceStateData(0, new TupleQueue[Tuple[Int]]))

  when(RUN) {
    case Event(tuple: Tuple[Int], data: InstanceStateData) => {
      stay() using (data.copy(num = data.num + 1))
    }
    case Event(migrationTable: MigrationTable, data: InstanceStateData) => {
      coordinatorRef = sender()
      goto(MIGRATION)
    }
  }

  when(MIGRATION) {
    case Event(Done, data: InstanceStateData) => {
      goto(RUN)
    }
  }

  onTransition {
    case RUN -> MIGRATION => {
      // migration procedure


      self ! Done
    }
    case MIGRATION -> RUN => {
      coordinatorRef ! MigrationCompleted
    }
  }
}

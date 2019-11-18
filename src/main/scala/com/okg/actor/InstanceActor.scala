package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message.{Done, MigrationCompleted, MigrationTable}
import com.okg.state._
import com.okg.tuple.{Tuple, TupleQueue}

/**
  * Class for Operator instance
  */
class InstanceActor extends Actor with FSM[InstanceState, InstanceStateData] {

  var coordinatorActorRef = Actor.noSender
  startWith(RUN, new InstanceStateData(0, new TupleQueue[Tuple[Int]]))

  when(RUN) {
    case Event(tuple: Tuple[Int], data: InstanceStateData) => {
      // enqueue tuple
      stay() using (data.copy(tupleNums = data.tupleNums + 1))
    }

    case Event(migrationTable: MigrationTable, data: InstanceStateData) => {
      coordinatorActorRef = sender()
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
      coordinatorActorRef ! MigrationCompleted
    }
  }
}

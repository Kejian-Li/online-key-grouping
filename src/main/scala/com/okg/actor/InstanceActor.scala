package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message.{Done, MigrationCompleted}
import com.okg.state._
import com.okg.tuple.{Tuple, TupleQueue}

class InstanceActor(coordinatorActor: ActorRef) extends Actor with FSM[InstanceState, InstanceStateData] {

  startWith(RUN, new InstanceStateData(0, new TupleQueue[Tuple[Int]]))

  when(RUN) {
    case Event(tuple: Tuple[Int], data: InstanceStateData) => {
      stay() using (data.copy(num = data.num + 1))
    }
  }

  when(MIGRATION) {
    case Event(Done, _) => {
      goto(RUN)
    }
  }

  onTransition {
    case RUN -> MIGRATION => {
      // migration procedure
      self ! Done
    }
    case MIGRATION -> RUN => {
      coordinatorActor ! MigrationCompleted
    }
  }
}

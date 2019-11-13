package com.okg.actor

import akka.actor.{Actor, FSM}
import com.okg.state.{InstanceState, RUN}
import com.okg.tuple.{Tuple, TupleQueue}

class InstanceActor extends Actor with FSM[InstanceState, TupleQueue[Tuple[Int]]] {

  private var num = 0

  when(RUN) {
    case Event(tuple: Tuple[Int], _) => {
      num += 1
    }
  }
}

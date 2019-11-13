package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.state.{CoordinatorState, GENERATION, WAIT_ALL}

import scala.collection.mutable

class CoordinatorActor extends Actor with FSM[CoordinatorState, MessageQueue[Message]] {
  private val k = 5
  private var sketches = 0
  private var notification = 0

  private var routingTable: RoutingTable = _
  private val messageQueue: MessageQueue[Message] = _

  private val actor: ActorRef = _

  startWith(WAIT_ALL, messageQueue)

  when(WAIT_ALL) {
    case Event(sketch: Sketch, messageQueue: MessageQueue[Message]) => {
      sketches += 1
      if (sketches == k) {
        sketches = 0
        goto(GENERATION)
      }
    }
  }

  when(GENERATION) {
    case Event(MigrationCompleted, _) => {
      notification += 1
      if (notification == k) {
        actor ! routingTable
      }
    }
  }


  onTransition {
    case _ -> GENERATION => {
      routingTable = generateRoutingTable()
      val migrationTable = makeMigrationTable()
      for (i <- 1 to k) {
        actor ! migrationTable
      }
    }
    case _ -> WAIT_ALL => {
      notification = 0
      sketches = 0
    }
  }

  def generateRoutingTable(): RoutingTable = {
    RoutingTable()
  }

  def makeMigrationTable(): mutable.HashMap[Int, Int] = {

    MigrationTable
  }
}

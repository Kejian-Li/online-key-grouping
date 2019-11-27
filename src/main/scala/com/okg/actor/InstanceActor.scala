package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.message.communication.{MigrationCompleted, StartSimulation, TerminateSimulation}
import com.okg.message.registration.{CoordinatorRegistration, StatisticsActorRegistration}
import com.okg.state._
import com.okg.tuple.Tuple

import scala.collection.mutable

/**
  * Class for Operator instance
  */
class InstanceActor(index: Int) extends Actor with FSM[InstanceState, InstanceStateData] {

  var coordinatorActorRef = Actor.noSender
  var instanceActors = Array.empty[ActorRef]
  var periodTuplesNum = 0

  startWith(RUN, new InstanceStateData(0, mutable.Map.empty[Int, Int]))

  when(RUN) {
    case Event(tuple: Tuple[Int], data: InstanceStateData) => {
      // process tuple
      periodTuplesNum += 1
      val key = tuple.key
      data.tupleMap.update(key, data.tupleMap.getOrElse(key, 0) + 1) // virtual, no real meaning
      stay() using (data.copy(tuplesNum = data.tuplesNum + 1))
    }

    case Event(startMigration: StartMigration, data: InstanceStateData) => {
      instanceActors = startMigration.instanceActors
      goto(MIGRATION)
    }
  }

  var period = 1
  when(MIGRATION) {
    case Event(MigrationCompleted, data: InstanceStateData) => {
      log.info("Instance " + index + " migrates successfully")
      statisticsActor ! new Statistics(index, period, periodTuplesNum, data.tuplesNum)

      period += 1
      periodTuplesNum = 0
      log.info("Instance " + index + " is gonna enter period " + period)
      goto(RUN)
    }
  }

  var statisticsActor = Actor.noSender

  val schedulerActorsSet = mutable.Set.empty[ActorRef]
  var receivedTerminationNotification = 0
  whenUnhandled {
    case Event(StartSimulation, data: InstanceStateData) => {
      schedulerActorsSet.add(sender())
      stay()
    }
    case Event(CoordinatorRegistration, data: InstanceStateData) => {
      coordinatorActorRef = sender()
      stay()
    }

    case Event(StatisticsActorRegistration, data: InstanceStateData) => {
      statisticsActor = sender()
      stay()
    }

    // there is a logic bug
    case Event(TerminateSimulation, data: InstanceStateData) => {
      receivedTerminationNotification += 1
      if (receivedTerminationNotification == schedulerActorsSet.size) {
        sender() ! new Load(index, data.tuplesNum) // tell simulation actor statistics
      }
      stay()
    }
  }

  onTransition {
    case RUN -> MIGRATION => {
      // migration procedure. it is omitted. we simply suppose that migration is completed
      self ! MigrationCompleted
    }
    case MIGRATION -> RUN => {
      coordinatorActorRef ! MigrationCompleted
    }
  }
}

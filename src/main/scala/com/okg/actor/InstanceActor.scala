package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.message.communication.{MigrationCompleted, StartSimulation, TerminateSimulation}
import com.okg.message.registration.{CompilerRegistrationAtInstance, StatisticsRegistrationAtInstance}
import com.okg.message.statistics.{InstanceStatistics, Load}
import com.okg.state._
import com.okg.tuple.Tuple

import scala.collection.mutable

/**
  * Actor for Operator instance
  */
class InstanceActor(index: Int) extends Actor with FSM[InstanceState, InstanceStateData] {

  var compilerActorRef = Actor.noSender
  val schedulerActorsSet = mutable.Set.empty[ActorRef]
  var instanceActors = Array.empty[ActorRef]
  var receivedPeriodTuplesNum = 0

  startWith(RUN, new InstanceStateData(0, 0, mutable.Map.empty[Int, Int], null))

  var receivedPeriodBarriersNum = 0
  when(RUN) {
    case Event(tuple: Tuple[Int], data: InstanceStateData) => {
      // process tuple
      receivedPeriodTuplesNum += 1
      val key = tuple.key
      data.tupleMap.update(key, data.tupleMap.getOrElse(key, 0) + 1) // virtual, no real meaning

      stay() using (data.copy(tuplesNum = data.tuplesNum + 1))
    }

    case Event(periodBarrier: PeriodBarrier, data: InstanceStateData) => {
      assert(periodBarrier.period == data.period)

      receivedPeriodBarriersNum += 1
      if (receivedPeriodBarriersNum == schedulerActorsSet.size) {

        log.info("Instance " + index + " received all the barriers and sends statistic of period " + periodBarrier.period)
        statisticsActor ! new InstanceStatistics(index, data.period, receivedPeriodTuplesNum, data.tuplesNum)

        receivedPeriodTuplesNum = 0
        receivedPeriodBarriersNum = 0
      }
      stay()
    }

    case Event(startMigration: StartMigration, data: InstanceStateData) => {
      goto(MIGRATE) using (data.copy(migrationTable = startMigration.migrationTable))
    }
  }

  when(MIGRATE) {
    case Event(MigrationCompleted, data: InstanceStateData) => {
      log.info("Instance " + index + " migrates successfully")

      compilerActorRef ! MigrationCompleted
      goto(RUN) using (data.copy(period = data.period + 1))
    }
  }

  var statisticsActor = Actor.noSender

  var receivedTerminationNotification = 0
  whenUnhandled {
    case Event(StartSimulation, data: InstanceStateData) => {
      schedulerActorsSet.add(sender())
      stay()
    }
    case Event(CompilerRegistrationAtInstance, data: InstanceStateData) => {
      compilerActorRef = sender()
      stay()
    }

    case Event(StatisticsRegistrationAtInstance, data: InstanceStateData) => {
      statisticsActor = sender()
      stay()
    }

    case Event(TerminateSimulation, data: InstanceStateData) => {

      receivedTerminationNotification += 1
      if (receivedTerminationNotification == schedulerActorsSet.size) {
        log.info("Instance " + index + " received all the termination notifications")
        sender() ! new Load(index, data.tuplesNum) // tell simulation actor statistics
      }
      stay()
    }
  }

  onTransition {
    case RUN -> MIGRATE => {
      log.info("Instance " + index + " enters MIGRATE")
      // migration procedure. it is omitted. we simply suppose that migration is completed
      self ! MigrationCompleted
    }

    case MIGRATE -> RUN => {
      log.info("Instance " + index + " enters " + nextStateData.period + " RUNs")
    }
  }
}

package com.okg.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.okg.message.Statistics
import com.okg.message.communication.StartSimulation
import com.okg.message.registration.StatisticsActorRegistration

class StatisticsActor(instanceActors: Array[ActorRef]) extends Actor with ActorLogging {

  override def receive: Receive = {
    case StartSimulation => {
      instanceActors.foreach{
        instanceActor => {
          instanceActor ! StatisticsActorRegistration
        }
      }
    }

    case Statistics(index, period, tupleNums) => {
      log.info("Statistic: instance " + index + " at " + period + " is " + tupleNums)
    }

  }

}

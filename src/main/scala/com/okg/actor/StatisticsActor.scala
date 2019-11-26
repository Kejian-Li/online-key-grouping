package com.okg.actor

import java.io.{File, PrintWriter}

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.okg.message.Statistics
import com.okg.message.communication.StartSimulation
import com.okg.message.registration.StatisticsActorRegistration

class StatisticsActor(instanceActors: Array[ActorRef]) extends Actor with ActorLogging {

  val instanceSize = instanceActors.size
  val periodWriters = new Array[PrintWriter](instanceSize)

  // initialize
  override def preStart(): Unit = {
    for (i <- 0 to instanceSize - 1) {
      val fileName = "instance_" + i
      val file = new File(fileName)
      if(file.exists()) {
        file.delete()
      }
      periodWriters(i) = new PrintWriter(new File(fileName))
    }
  }

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
      periodWriters(index).println(period + " " + tupleNums)
      periodWriters(index).flush()
    }

  }

}

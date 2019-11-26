package com.okg.actor

import java.io.{File, PrintWriter}

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.csvreader.CsvWriter
import com.okg.message.Statistics
import com.okg.message.communication.StartSimulation
import com.okg.message.registration.StatisticsActorRegistration

class StatisticsActor(instanceActors: Array[ActorRef]) extends Actor with ActorLogging {

  val instanceSize = instanceActors.size
  val periodWriters = new Array[CsvWriter](instanceSize)

  var instanceResultDirectory: File = new File("instance_statistics_output")
  // initialize
  override def preStart(): Unit = {
    if(!instanceResultDirectory.exists()) {
      instanceResultDirectory.mkdir()
    }
    for (i <- 0 to instanceSize - 1) {
      val fileName = instanceResultDirectory.getCanonicalPath + "/instance_" + i + ".csv"
      val file = new File(fileName)
      if(file.exists()) {
        file.delete()
      }
      periodWriters(i) = new CsvWriter(fileName)
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
      val record = new Array[String](2)
      record(0) = period.toString
      record(1) = tupleNums.toString
      periodWriters(index).writeRecord(record)
      periodWriters(index).flush()
    }

  }

  override def postStop(): Unit = {
    periodWriters.foreach{
      periodWriter => {
        periodWriter.close()
      }
    }

  }

}

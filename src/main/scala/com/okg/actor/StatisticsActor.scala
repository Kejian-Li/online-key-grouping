package com.okg.actor

import java.io.File

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
    if (!instanceResultDirectory.exists()) {
      instanceResultDirectory.mkdir()
    } else {
      val instanceFiles = instanceResultDirectory.listFiles()
      instanceFiles.forall {
        instanceFile => instanceFile.delete()
      }
    }
    for (i <- 0 to instanceSize - 1) {
      val fileName = instanceResultDirectory.getCanonicalPath + "/instance_" + i + ".csv"
      periodWriters(i) = new CsvWriter(fileName)
    }
  }

  var receivedTupleSum = 0
  var instancesNum = 0

  override def receive: Receive = {
    case StartSimulation => {
      instanceActors.foreach {
        instanceActor => {
          instanceActor ! StatisticsActorRegistration
        }
      }
    }

    case Statistics(index, period, periodTuplesNum, totalTuplesNum) => {
      log.info("Statistic: instance " + index + " at " + period + " received "
        + periodTuplesNum + ", " + totalTuplesNum + " in total")
      instancesNum += 1
      receivedTupleSum += periodTuplesNum
      if(instancesNum == instanceActors.length) {
        log.info("Statistic: all the instances received " + receivedTupleSum)
        instancesNum = 0
      }
      val record = new Array[String](2)
      record(0) = period.toString
      record(1) = totalTuplesNum.toString
      periodWriters(index).writeRecord(record)
      periodWriters(index).flush()
    }

  }

  override def postStop(): Unit = {
    periodWriters.foreach {
      periodWriter => {
        periodWriter.close()
      }
    }

  }

}

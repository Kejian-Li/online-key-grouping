package com.okg.actor

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.csvreader.CsvWriter
import com.okg.message.communication.StartSimulation
import com.okg.message.registration.{StatisticsRegistrationAtCompiler, StatisticsRegistrationAtInstances}
import com.okg.message.statistics.{CompilerStatistics, InstanceStatistics}

class StatisticsActor(instanceActors: Array[ActorRef],
                      compilerActor: ActorRef) extends Actor with ActorLogging {

  val instanceSize = instanceActors.size
  val instanceWriters = new Array[CsvWriter](instanceSize)
  var compilerWriter: CsvWriter = null

  var instancesStatisticsDirectory: File = new File("instances_statistics_output")
  var compilerStatisticsDirectory: File = new File("compiler_statistics_output")

  // initialize
  override def preStart(): Unit = {
    if (!instancesStatisticsDirectory.exists()) {
      instancesStatisticsDirectory.mkdir()
    } else {
      val instanceFiles = instancesStatisticsDirectory.listFiles()
      instanceFiles.forall {
        instanceFile => instanceFile.delete()
      }
    }
    for (i <- 0 to instanceSize - 1) {
      val instanceFileName = instancesStatisticsDirectory.getCanonicalPath + "/instance_" + i + ".csv"
      instanceWriters(i) = new CsvWriter(instanceFileName)
    }
    if(!compilerStatisticsDirectory.exists()) {
      compilerStatisticsDirectory.mkdir()
    }
    val compilerFileName = compilerStatisticsDirectory.getCanonicalPath +  "/compiler_output.csv"
    val compilerFile = new File(compilerFileName)
    if(compilerFile.exists()) {
      compilerFile.delete()
    }
    compilerWriter = new CsvWriter(compilerFileName)
  }

  var receivedTupleSum = 0
  var instancesNum = 0

  override def receive: Receive = {
    case StartSimulation => {
      instanceActors.foreach {
        instanceActor => {
          instanceActor ! StatisticsRegistrationAtInstances
        }
      }
      compilerActor ! StatisticsRegistrationAtCompiler
    }

    case InstanceStatistics(index, period, periodTuplesNum, totalTuplesNum) => {
      log.info("Statistic: instance " + index + " received "
        + periodTuplesNum + " at period " + period + ", " + totalTuplesNum + " in total")
      instancesNum += 1
      receivedTupleSum += periodTuplesNum
      if (instancesNum == instanceActors.length) {
        log.info("Statistic: all the instances received " + receivedTupleSum + " so far")
        instancesNum = 0
      }
      val instanceRecord = new Array[String](2)
      instanceRecord(0) = period.toString
      instanceRecord(1) = totalTuplesNum.toString
      instanceWriters(index).writeRecord(instanceRecord)
      instanceWriters(index).flush()

    }

    case CompilerStatistics(period: Int,
                            routingTableGenerationTime: Long,
                            routingTableSize: Int,
                            migrationTableSize: Int) => {
      log.info("Statistic: compiler takes " + routingTableGenerationTime + " to generate next routing table"
        + " at period " + period + ", " + " its size is " + routingTableSize + ", " + "migration table size is "
        + migrationTableSize)
      val compilerRecord = new Array[String](3)
      compilerRecord(0) = routingTableGenerationTime.toString
      compilerRecord(1) = routingTableSize.toString
      compilerRecord(2) = migrationTableSize.toString
      compilerWriter.writeRecord(compilerRecord)
      compilerWriter.flush()
    }

  }

  override def postStop(): Unit = {
    instanceWriters.foreach {
      periodWriter => {
        periodWriter.close()
      }
    }

  }

}

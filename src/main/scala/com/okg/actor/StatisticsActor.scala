package com.okg.actor

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.csvreader.CsvWriter
import com.okg.message.registration.{StatisticsRegistrationAtCompiler, StatisticsRegistrationAtInstance, StatisticsRegistrationAtScheduler}
import com.okg.message.statistics.{CompilerStatistics, InstanceStatistics, SchedulerStatistics}

class StatisticsActor(schedulerActors: Array[ActorRef],
                      instanceActors: Array[ActorRef],
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
    if (!compilerStatisticsDirectory.exists()) {
      compilerStatisticsDirectory.mkdir()
    }
    val compilerFileName = compilerStatisticsDirectory.getCanonicalPath + "/compiler_z=3.0_.csv"
    val compilerFile = new File(compilerFileName)
    if (compilerFile.exists()) {
      compilerFile.delete()
    }
    compilerWriter = new CsvWriter(compilerFileName)

    // registration itself
    schedulerActors.foreach {
      schedulerActor => {
        schedulerActor ! StatisticsRegistrationAtScheduler
      }
    }
    compilerActor ! StatisticsRegistrationAtCompiler
    instanceActors.foreach {
      instanceActor => {
        instanceActor ! StatisticsRegistrationAtInstance
      }
    }

  }

  var receivedTuplesSum = 0
  var instancesNum = 0

  var schedulerAveragePeriodDelayTime = new Array[Long](schedulerActors.size)

  override def receive: Receive = {

    case SchedulerStatistics(index: Int, totalPeriod: Int, averagePeriodDelay: Long) => {
      schedulerAveragePeriodDelayTime(index) = averagePeriodDelay
    }

    case CompilerStatistics(period: Int,
    routingTableGenerationTime: Long,
    routingTableSize: Int,
    migrationTableSize: Int) => {
      log.info("Statistic: compiler takes " + routingTableGenerationTime + " microseconds to generate next routing table"
        + " at period " + period + ", " + " its size is " + routingTableSize + ", " + "migration table size is "
        + migrationTableSize)
      val compilerRecord = new Array[String](3)
      compilerRecord(0) = routingTableGenerationTime.toString
      compilerRecord(1) = routingTableSize.toString
      compilerRecord(2) = migrationTableSize.toString
      compilerWriter.writeRecord(compilerRecord)
      compilerWriter.flush()
    }

    case InstanceStatistics(index, period, periodTuplesNum, totalTuplesNum) => {
      log.info("Statistic: instance " + index + " received "
        + periodTuplesNum + " at period " + period + ", " + totalTuplesNum + " in total")
      instancesNum += 1
      receivedTuplesSum += periodTuplesNum
      if (instancesNum == instanceActors.length) {
        log.info("Statistic: all the instances received " + receivedTuplesSum + " so far")
        instancesNum = 0
      }
      val instanceRecord = new Array[String](2)
      instanceRecord(0) = period.toString
      instanceRecord(1) = totalTuplesNum.toString
      instanceWriters(index).writeRecord(instanceRecord)
      instanceWriters(index).flush()

    }

  }

  override def postStop(): Unit = {
    schedulerAveragePeriodDelayTime.sum / schedulerActors.size
    log.info("Statistics: schedulers' Average Period Delay Time is " + schedulerAveragePeriodDelayTime)
    instanceWriters.foreach {
      periodWriter => {
        periodWriter.close()
      }
    }

  }

}

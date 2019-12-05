package com.okg.actor

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.csvreader.CsvWriter
import com.okg.message.registration.{StatisticsRegistrationAtCompiler, StatisticsRegistrationAtInstance, StatisticsRegistrationAtScheduler}
import com.okg.message.statistics.{CompilerStatistics, InstanceStatistics}
import org.apache.commons.math3.util.FastMath

class StatisticsActor(schedulerActors: Array[ActorRef],
                      instanceActors: Array[ActorRef],
                      compilerActor: ActorRef) extends Actor with ActorLogging {

  val instanceSize = instanceActors.size
  var instanceWriter: CsvWriter = null
  var compilerWriter: CsvWriter = null

  var instancesStatisticsDirectory: File = new File("instances_statistics_output")
  var compilerStatisticsDirectory: File = new File("compiler_statistics_output")

  // initialize
  override def preStart(): Unit = {

    // registration itself
    compilerActor ! StatisticsRegistrationAtCompiler
    instanceActors.foreach {
      instanceActor => {
        instanceActor ! StatisticsRegistrationAtInstance
      }
    }

    // output
    if (!instancesStatisticsDirectory.exists()) {
      instancesStatisticsDirectory.mkdir()
    } else {
      val instanceFiles = instancesStatisticsDirectory.listFiles()
      instanceFiles.forall {
        instanceFile => instanceFile.delete()
      }
    }
    val instanceFileName = instancesStatisticsDirectory.getCanonicalPath + "/instances_output" + ".csv"
    instanceWriter = new CsvWriter(instanceFileName)

    if (!compilerStatisticsDirectory.exists()) {
      compilerStatisticsDirectory.mkdir()
    }
    val compilerFileName = compilerStatisticsDirectory.getCanonicalPath + "/compiler_z=1.0_.csv"
    val compilerFile = new File(compilerFileName)
    if (compilerFile.exists()) {
      compilerFile.delete()
    }
    compilerWriter = new CsvWriter(compilerFileName)

  }

  var receivedInstancesEachPeriod = 0
  var instanceBuckets = new Array[Int](instanceSize)

  override def receive: Receive = {

    case CompilerStatistics(period: Int,
    routingTableGenerationTime: Long,
    routingTableSize: Int,
    migrationTableSize: Int) => {
      log.info("Statistic: compiler takes " + routingTableGenerationTime + " microseconds to generate next routing table"
        + " at period " + period + ", " + " its size is " + routingTableSize + ", " + "migration table size is "
        + migrationTableSize)
      val compilerRecord = new Array[String](4)
      compilerRecord(0) = period.toString
      compilerRecord(1) = routingTableGenerationTime.toString
      compilerRecord(2) = routingTableSize.toString
      compilerRecord(3) = migrationTableSize.toString
      compilerWriter.writeRecord(compilerRecord)
      compilerWriter.flush()
    }

    case InstanceStatistics(index, period, periodTuplesNum, totalTuplesNum) => {
      log.info("Statistic: instance " + index + " received "
        + periodTuplesNum + " at period " + period + ", " + totalTuplesNum + " in total")
      receivedInstancesEachPeriod += 1

      instanceBuckets(index) += periodTuplesNum
      if (receivedInstancesEachPeriod == instanceSize) {
        var receivedTuplesSum = instanceBuckets(0)
        // imbalance
        var maxLoad = instanceBuckets(0)
        for (i <- 1 to instanceSize - 1) {
          if (maxLoad < instanceBuckets(i)) {
            maxLoad = instanceBuckets(i)
          }
          receivedTuplesSum += instanceBuckets(i)
        }
        val averageLoad = receivedTuplesSum / instanceSize
        val imbalance: Float = ((maxLoad.toFloat / averageLoad.toFloat) - 1) * 100

        // sigma
        var squareSum = 0.0d
        for (i <- 0 to instanceSize - 1) {
          val difference = instanceBuckets(i) - averageLoad
          val square = FastMath.pow(difference.toDouble, 2)
          squareSum += square
        }
        val sigma = math.sqrt(squareSum / instanceSize)

        val instanceRecord = new Array[String](3)
        instanceRecord(0) = period.toString
        instanceRecord(1) = imbalance.toString
        instanceRecord(2) = sigma.toString
        instanceWriter.writeRecord(instanceRecord)
        instanceWriter.flush()

        receivedInstancesEachPeriod = 0
      }

    }

  }

  override def postStop(): Unit = {
    compilerWriter.close()
    instanceWriter.close()
  }

}

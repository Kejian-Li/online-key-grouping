package com.okg.main

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.csvreader.CsvReader
import com.okg.message.communication.{CompletenessAsk, CompletenessReply, StartSimulation, TerminateSimulation}
import com.okg.message.statistics.{Load, SchedulerStatistics}
import com.okg.tuple.Tuple
import org.apache.commons.math3.util.FastMath

import scala.concurrent.duration.Duration

/**
  * Simulation Actor with main simulation logic
  *
  * @param coordinatorActor
  * @param schedulerActors
  * @param instanceActors
  */
class SimulationActor(coordinatorActor: ActorRef,
                      schedulerActors: Array[ActorRef],
                      instanceActors: Array[ActorRef]) extends Actor with ActorLogging {

  val s = schedulerActors.size
  val k = instanceActors.size
  var loads = new Array[Int](k)

  val windowsFileName = "C:\\Users\\lizi\\Desktop\\OKG_Workspace\\OKG_data\\Unfixed_Distribution" +
    "\\zipf_z_unfixed_data.csv"
  val ubuntuFileName = "/home/lizi/workspace/okg_workspace/zipf_data/zipf_z_2-0.csv"
  val tupleNums = new Array[Int](s)

  def startSimulation(): Unit = {
    val inFileName = windowsFileName

    val csvItemReader = new CsvItemReader(new CsvReader(inFileName))
    var items = csvItemReader.nextItem()

    // Simulation starts...
    for (i <- 0 to s - 1) {
      schedulerActors(i) ! StartSimulation
    }

    var sourceIndex = 0

    while (items != null) {
      for (i <- 0 to items.size - 1) {
        schedulerActors(sourceIndex) ! new Tuple[Int](items(i).toInt)
        tupleNums(sourceIndex) += 1

        sourceIndex += 1
        if (sourceIndex == s) {
          sourceIndex = 0
        }
      }
      items = csvItemReader.nextItem()
    }

    // Simulation terminates...
    log.info("Simulator: send simulation termination notification<><><><><><><><><><>")
    for (i <- 0 to s - 1) {
      schedulerActors(i) ! TerminateSimulation
    }
  }

  var schedulersTotalTupleNum = 0
  var instancesTotalTupleNum = 0

  var startTime = 0L
  var endTime = 0L

  var receivedLoad = 0

  var main: ActorRef = null

  var receivedSchedulersFinalStatistics = 0

  var schedulersAveragePeriodDelayTime = new Array[Long](s)

  override def receive: Receive = {
    case StartSimulation => {
      log.info("Simulator: simulation starts...")
      startTime = System.currentTimeMillis()
      startSimulation()
    }

    case Load(index, x) => {
      loads(index) = x
      receivedLoad += 1
      if (receivedLoad == k) {
        endTime = System.currentTimeMillis()
        log.info("Simulator: received all loads")
        log.info("Simulator: simulation terminates...")
        log.info("Simulator: simulation takes " + (endTime - startTime) + " ms")
        log.info("Simulator: now output statistics: ")

        log.info("Simulator: schedulers:")
        for (i <- 0 to s - 1) {
          schedulersTotalTupleNum += tupleNums(i)
          log.info(i + "  ->  " + tupleNums(i))
        }
        log.info("Simulator: Schedulers received " + schedulersTotalTupleNum + " tuples in total")

        log.info("Simulator: instances:")
        for (i <- 0 to k - 1) {
          instancesTotalTupleNum += loads(i)
          log.info(i + "  ->  " + loads(i))
        }
        log.info("Simulator: Instances received " + instancesTotalTupleNum + " tuples in total")

        log.info(" ")

        if (receivedSchedulersFinalStatistics == s) {
          val finalAverageDelayTime = schedulersAveragePeriodDelayTime.sum / schedulerActors.size
          val finalMillisecondsDelayTime = Duration.fromNanos(finalAverageDelayTime).toMillis
          log.info("Simulator: schedulers' Average Period Delay Time is " + finalMillisecondsDelayTime + " ms")
        }

        var maxLoad = loads(0)
        for (i <- 1 to k - 1) {
          if (maxLoad < loads(i)) {
            maxLoad = loads(i)
          }
        }

        val averageLoad = instancesTotalTupleNum / k
        log.info("Simulator: average load of each load is " + averageLoad)
        val imbalance: Float = ((maxLoad.toFloat / averageLoad.toFloat) - 1) * 100
        log.info("Simulator: Final imbalance is " + imbalance + "%")
        var squareSum = 0.0d
        for (i <- 0 to k - 1) {
          val difference = loads(i) - averageLoad
          val square = FastMath.pow(difference.toDouble, 2)
          squareSum += square
        }
        val sigma = math.sqrt(squareSum / k)
        log.info("Simulator: Final standard deviation is " + sigma)
        main ! CompletenessReply
      }
    }

    case SchedulerStatistics(index: Int, totalPeriod: Int, averagePeriodDelay: Long) => {
      schedulersAveragePeriodDelayTime(index) = averagePeriodDelay
      receivedSchedulersFinalStatistics += 1
    }

    case CompletenessAsk => {
      main = sender()
    }
  }

}

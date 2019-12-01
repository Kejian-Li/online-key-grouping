package com.okg.main

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.csvreader.CsvReader
import com.okg.message.Load
import com.okg.message.communication.{StartSimulation, TerminateSimulation}
import com.okg.tuple.Tuple

/**
  * Simulation Actor with main simulation logic
  *
  * @param coordinatorActor
  * @param schedulerActors
  * @param instanceActors
  */
class SimulationActor(coordinatorActor: ActorRef,
                      schedulerActors: Array[ActorRef],
                      instanceActors: Array[ActorRef],
                      statisticsActor: ActorRef) extends Actor with ActorLogging {

  val s = schedulerActors.size
  val k = instanceActors.size
  var loads = new Array[Int](k)

  val windowsFileName = "C:\\Users\\lizi\\Desktop\\thesis_workspace\\OKG_workspace\\OKG_data\\" +
    "Zipf_Data\\Fixed_Distribution\\zipf_z_2-0.csv"
  val ubuntuFileName = "/home/lizi/workspace/scala_workspace/zipf_data/zipf_z_3-0.csv"
  val tupleNums = new Array[Int](s)

  def startSimulation(): Unit = {
    val inFileName = ubuntuFileName

    val csvItemReader = new CsvItemReader(new CsvReader(inFileName))
    var items = csvItemReader.nextItem()

    // Simulation starts...
    for (i <- 0 to s - 1) {
      schedulerActors(i) ! StartSimulation
    }

    statisticsActor ! StartSimulation

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
    log.info("Send simulation termination notification<><><><><><><><><><>")
    for (i <- 0 to s - 1) {
      schedulerActors(i) ! TerminateSimulation
    }
  }

  var schedulersTotalTupleNum = 0
  var instancesTotalTupleNum = 0

  var startTime = 0L
  var endTime = 0L

  var receivedLoad = 0

  override def receive: Receive = {
    case StartSimulation => {
      log.info("Simulation starts...")
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
        log.info("Simulator: Scheduler received " + schedulersTotalTupleNum + " tuples in total:")

        log.info("Simulator: instances:")
        for (i <- 0 to k - 1) {
          instancesTotalTupleNum += loads(i)
          log.info(i + "  ->  " + loads(i))
        }
        log.info("Simulator: instances received " + instancesTotalTupleNum + " tuples in total:")

        var maxLoad = loads(0)
        for (i <- 1 to k - 1) {
          if (maxLoad < loads(i)) {
            maxLoad = loads(i)
          }
        }
        val averageLoad = instancesTotalTupleNum / k
        val imbalance: Float = ((maxLoad.toFloat / averageLoad.toFloat) - 1) * 100
        log.info("Final imbalance is " + imbalance + "%")
      }
    }
  }

}

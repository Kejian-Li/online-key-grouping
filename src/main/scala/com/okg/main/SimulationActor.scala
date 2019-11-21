package com.okg.main

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.csvreader.CsvReader
import com.okg.message.{Load, StartSimulation, TerminateSimulation}
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
                      instanceActors: Array[ActorRef]) extends Actor with ActorLogging {

  val s = schedulerActors.size
  val k = instanceActors.size
  var loads = new Array[Int](k)
  var receivedLoad = 0

  def startSimulation(): Unit = {
    val inFileName =
      "C:\\Users\\lizi\\Desktop\\分布式流处理系统的数据分区算法研究\\dataset\\zipf_dataset\\zipf_z_1-2.csv"
    val csvItemReader = new CsvItemReader(new CsvReader(inFileName))
    var item = csvItemReader.nextItem()
    var sourceIndex = 0

    // Simulation starts...
    for (i <- 0 to s - 1) {
      schedulerActors(i) ! StartSimulation
    }

    while (item != null) {
      for (i <- 0 to item.size - 1) {
        schedulerActors(sourceIndex) ! new Tuple[Int](item(i).toInt)

        sourceIndex += 1
        if (sourceIndex == s) {
          sourceIndex = 0
        }
      }
      item = csvItemReader.nextItem()
    }

    // Simulation terminates...
    for (i <- 0 to s - 1) {
      schedulerActors(i) ! TerminateSimulation
    }
  }

  override def receive: Receive = {
    case StartSimulation => {
      log.info("Simulation starts...")
      startSimulation()
    }

    case Load(index, x) => {
      loads(index) = x
      receivedLoad += 1
      if (receivedLoad == k) {
        log.info("received all loads")
        for (i <- 0 to k - 1) {
          log.info("instance " + i + " received " + loads(i) + " tuples")
        }
      }
    }
  }

}

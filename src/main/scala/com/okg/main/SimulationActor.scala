package com.okg.main

import akka.actor.{Actor, ActorRef}
import com.csvreader.CsvReader
import com.okg.message.{Load, SimulationDone}
import com.okg.tuple.Tuple

/**
  * Simulation Actor with main simulation logic
  * @param coordinatorActor
  * @param schedulerActors
  * @param instanceActors
  */
class SimulationActor(coordinatorActor: ActorRef,
                      schedulerActors: Array[ActorRef],
                      instanceActors: Array[ActorRef]) extends Actor {

  var load: Int = 0

  def startSimulation(): Unit = {
    val inFileName =
      "C:\\Users\\lizi\\Desktop\\分布式流处理系统的数据分区算法研究\\dataset\\zipf_dataset\\zipf_z_0-8.csv"
    val csvItemReader = new CsvItemReader(new CsvReader(inFileName))
    var item = csvItemReader.nextItem()
    var sourceIndex = 0

    while (item != null) {
      for (i <- 0 to item.size - 1) {
        schedulerActors(sourceIndex) ! new Tuple[Int](item(i).toInt)

        sourceIndex += 1
        if (sourceIndex == schedulerActors.size) {
          sourceIndex = 0
        }
      }
      item = csvItemReader.nextItem()
    }

    for (i <- 0 to instanceActors.size - 1) {
      instanceActors(i) ! SimulationDone
    }
  }

  override def receive: Receive = {
    case Load(index, x) => {
      load += x
    }
  }

}

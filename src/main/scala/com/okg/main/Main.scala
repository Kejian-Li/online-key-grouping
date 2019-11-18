package com.okg.main

import akka.actor.{ActorRef, ActorSystem, Props}
import com.csvreader.CsvReader
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor}
import com.okg.tuple.Tuple

object Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem()

    val N = 1000
    val m = 10000
    val s = 2
    val k = 3
    val epsilon = 0.05
    val theta = 0.01


    val schedulerActors = new Array[ActorRef](s)

    val instanceActors = new Array[ActorRef](k)
    for (i <- 0 to k - 1) {
      instanceActors(i) = system.actorOf(Props(new InstanceActor()))
    }

    val coordinatorActorRef = system.actorOf(Props(new CoordinatorActor(instanceActors, s, k)))

    for (i <- 0 to s - 1) {
      schedulerActors(i) =
        system.actorOf(Props(new SchedulerActor(i, N, m, k, epsilon, theta, coordinatorActorRef, instanceActors)))
    }

    val inFileName =
      "C:\\Users\\lizi\\Desktop\\分布式流处理系统的数据分区算法研究\\dataset\\zipf_dataset\\zipf_z_0-8.csv"
    val csvItemReader = new CsvItemReader(new CsvReader(inFileName))
    var item = csvItemReader.nextItem()
    var sourceIndex = 0

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
  }

}

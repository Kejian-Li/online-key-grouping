package com.okg.main

import akka.actor.{ActorRef, ActorSystem, Props}
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor}

class Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem()

    val N = 1000
    val m = 100000
    val s = 3
    val k = 5
    val epsilon = 0.05
    val theta = 0.01


    val schedulerActors = new Array[ActorRef](s)
    for (i <- 0 to s) {
      schedulerActors(i) = system.actorOf(Props(new SchedulerActor(N, m, k, epsilon, theta, coordinatorActorRef)))
    }

    val instanceActors = new Array[ActorRef](k)
    for (i <- 0 to k) {
      instanceActors(i) = system.actorOf(Props(new InstanceActor()))
    }

    val coordinatorActorRef = system.actorOf(Props(new CoordinatorActor(instanceActors, s, k)))


  }

}

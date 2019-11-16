package com.okg.main

import akka.actor.{ActorRef, ActorSystem, Props}
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor}

object Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem()

    val N = 1000
    val m = 100000
    val s = 3
    val k = 5
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
        system.actorOf(Props(new SchedulerActor(N, m, k, epsilon, theta, coordinatorActorRef, instanceActors)))
    }

  }

}

package com.okg.main

import akka.actor.{ActorRef, ActorSystem, Props}
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor}

object Main {

  def main(args: Array[String]): Unit = {

    val N = 1000
    val m = 10000
    val s = 2
    val k = 3
    val epsilon = 0.05
    val theta = 0.01

    val system = ActorSystem()

    val schedulerActors = new Array[ActorRef](s)

    val instanceActors = new Array[ActorRef](k)
    for (i <- 0 to k - 1) {
      instanceActors(i) = system.actorOf(Props(new InstanceActor(i)))
    }

    val coordinatorActorRef = system.actorOf(Props(new CoordinatorActor(instanceActors, s, k)))

    for (i <- 0 to s - 1) {
      schedulerActors(i) =
        system.actorOf(Props(new SchedulerActor(i, N, m, k, epsilon, theta, coordinatorActorRef, instanceActors)))
    }

    val simulationActor = new SimulationActor(coordinatorActorRef, schedulerActors, instanceActors)

    simulationActor.startSimulation()
  }

}

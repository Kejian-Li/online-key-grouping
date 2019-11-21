package com.okg.main

import akka.actor.{ActorRef, ActorSystem, Props}
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor}
import com.okg.message.StartSimulation

object Main {

  def main(args: Array[String]): Unit = {

    val N = 1000
    val m = 1000
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

    val my_mailbox = "akka.actor.my_mailbox"
    val coordinatorActorRef = system.actorOf(
      Props(new CoordinatorActor(s, instanceActors)).withMailbox(my_mailbox))

    for (i <- 0 to s - 1) {
      schedulerActors(i) =
        system.actorOf(
          Props(new SchedulerActor(i, N, m, k, epsilon, theta, coordinatorActorRef, instanceActors))
            .withMailbox(my_mailbox))
    }

    val simulationActor = system.actorOf(
      Props(new SimulationActor(coordinatorActorRef, schedulerActors, instanceActors))
        .withMailbox(my_mailbox))

    simulationActor ! StartSimulation
  }

}

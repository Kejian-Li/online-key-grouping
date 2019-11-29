package com.okg.main

import akka.actor.{ActorRef, ActorSystem, Props}
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor, StatisticsActor}
import com.okg.message.communication.StartSimulation

object Main {

  def main(args: Array[String]): Unit = {

    val m = 10000
    val s = 3
    val k = 10
    val theta = 0.01
    val epsilon = theta / 2   // satisfy: theta > epsilon

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
          Props(new SchedulerActor(i, m, k, epsilon, theta, coordinatorActorRef, instanceActors))
            .withMailbox(my_mailbox))
    }

    val statisticsActor = system.actorOf(Props(new StatisticsActor(instanceActors)))

    val simulationActor = system.actorOf(
      Props(new SimulationActor(coordinatorActorRef, schedulerActors, instanceActors, statisticsActor))
        .withMailbox(my_mailbox))

    simulationActor ! StartSimulation
  }

}

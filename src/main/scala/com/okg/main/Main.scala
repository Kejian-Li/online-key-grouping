package com.okg.main

import akka.actor.{ActorRef, ActorSystem, Props}
import com.okg.actor.{CompilerActor, InstanceActor, SchedulerActor, StatisticsActor}
import com.okg.message.communication.{CompleteAsk, StartSimulation}
import akka.pattern.ask
import scala.concurrent.duration._

object Main {

  def main(args: Array[String]): Unit = {

    val m = 10000
    // s should by divided exactly by the number of total tuples, for example 10^7 in our test
    val s = 1
    val k = 10
    val theta = 0.01
    val epsilon = theta / 2 // satisfy: theta > epsilon

    val system = ActorSystem()

    val schedulerActors = new Array[ActorRef](s)

    val instanceActors = new Array[ActorRef](k)
    for (i <- 0 to k - 1) {
      instanceActors(i) = system.actorOf(Props(new InstanceActor(i)))
    }

    val my_mailbox = "akka.actor.my_mailbox"
    val coordinatorActorRef = system.actorOf(
      Props(new CompilerActor(s, instanceActors)).withMailbox(my_mailbox))

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

    implicit val timeout = akka.util.Timeout(2 minutes)
    val future = simulationActor ? CompleteAsk
    if (!future.value.isEmpty) {
      
    } else {
      system.log.info("Ask reply is null or timeout")
    }
  }

}

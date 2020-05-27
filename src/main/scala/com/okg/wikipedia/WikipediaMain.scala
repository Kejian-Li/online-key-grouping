package com.okg.wikipedia

import akka.actor.{ActorRef, ActorSystem, Props}
import com.okg.wikipedia.actor.{CompilerActor, InstanceActor, SchedulerActor, StatisticsActor}
import com.okg.wikipedia.message.communication.{CompletenessAsk, StartSimulation}
import akka.pattern.ask
import scala.concurrent.duration._


object WikipediaMain {

  def main(args: Array[String]): Unit = {

    val learningLength = 1000
    // (s * m) should by divided exactly by the number of total tuples, for example 10^7 in our test
    val s = 4
    val k = 32  // k is related to whether simulation can be finished or not
    val theta = 0.01    // threshold of heavy hitters
    val epsilon = theta / 2 // satisfy: theta > epsilon

    val system = ActorSystem()

    val schedulerActors = new Array[ActorRef](s)

    val instanceActors = new Array[ActorRef](k)
    for (i <- 0 to k - 1) {
      instanceActors(i) = system.actorOf(Props(new InstanceActor(i)))
    }

    val my_mailbox = "akka.actor.my_mailbox"
    val compilerActor = system.actorOf(
      Props(new CompilerActor(s, instanceActors)).withMailbox(my_mailbox))

    for (i <- 0 to s - 1) {
      schedulerActors(i) =
        system.actorOf(
          Props(new SchedulerActor(i, learningLength, k, epsilon, theta, compilerActor, instanceActors))
            .withMailbox(my_mailbox))
    }

    val statisticsActor = system.actorOf(Props(new StatisticsActor(schedulerActors, instanceActors, compilerActor)))

    val simulationActor = system.actorOf(
      Props(new SimulationActor(compilerActor, schedulerActors, instanceActors))
        .withMailbox(my_mailbox))

    simulationActor ! StartSimulation

    implicit val timeout = akka.util.Timeout(5 minutes)
    val future = simulationActor ? CompletenessAsk

    future.onComplete {
      case scala.util.Success(value) => {
        system.log.info(" ")
        system.log.info("Main: " + value)
        system.stop(statisticsActor)
        system.stop(compilerActor)
        instanceActors.foreach {
          instanceActor => {
            system.stop(instanceActor)
          }
        }
        schedulerActors.foreach {
          schedulerActor => {
            system.stop(schedulerActor)
          }
        }
        system.log.info("Main: stop all actors")
        System.exit(0)
      }
      case scala.util.Failure(exception) => {
        system.log.info("Main: " + exception)
      }
    }(scala.concurrent.ExecutionContext.Implicits.global)

  }

}

package com.okg.main

import akka.actor.{ActorSystem, Props}
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor}

class Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem()

    val coordinatorActor = system.actorOf(Props(new CoordinatorActor))

    val schedulerActor = system.actorOf(Props(new SchedulerActor))

    val instanceActor = system.actorOf(Props(new InstanceActor))
  }

}

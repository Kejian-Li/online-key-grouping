package com.okg.test

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestKit
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor}
import com.okg.tuple.Tuple
import org.scalatest.WordSpecLike

class OKG_Test extends TestKit(ActorSystem("OKG")) with WordSpecLike {

  "OKG" must {
    "follow the flow" in {
      val N = 3
      val m = 5


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
          system.actorOf(Props(new SchedulerActor(i, N, m, k, epsilon, theta, coordinatorActorRef, instanceActors)))
      }

      schedulerActors(0) ! new Tuple[Int](1)
      schedulerActors(0) ! new Tuple[Int](2)
      schedulerActors(0) ! new Tuple[Int](3)

      // HASH -> COLLECT

      schedulerActors(0) ! new Tuple[Int](1)
      schedulerActors(0) ! new Tuple[Int](2)
      schedulerActors(0) ! new Tuple[Int](3)
      schedulerActors(0) ! new Tuple[Int](1)
      schedulerActors(0) ! new Tuple[Int](2)

      // COLLECT ->
      schedulerActors(0) ! new Tuple[Int](3)

    }
  }

}

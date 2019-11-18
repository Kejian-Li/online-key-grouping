package com.okg.test

import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor}
import com.okg.state.{COLLECT, HASH}
import com.okg.tuple.Tuple
import org.scalatest.WordSpecLike

class OKG_Test extends TestKit(ActorSystem("OKG")) with WordSpecLike {

  "OKG" must {
    "follow the flow" in {
      val N = 3
      val m = 5

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

      val stateProbe = TestProbe()
      schedulerActors(0) ! new SubscribeTransitionCallBack(stateProbe.ref)

      stateProbe.expectMsg(new CurrentState(schedulerActors(0), HASH))

      schedulerActors(0) ! new Tuple[Int](1)
      schedulerActors(0) ! new Tuple[Int](2)
      schedulerActors(0) ! new Tuple[Int](3)

      stateProbe.expectMsg(new Transition(schedulerActors(0), HASH, COLLECT))

      // HASH -> COLLECT

      schedulerActors(0) ! new Tuple[Int](4)
      schedulerActors(0) ! new Tuple[Int](5)
      schedulerActors(0) ! new Tuple[Int](6)
      schedulerActors(0) ! new Tuple[Int](7)
      schedulerActors(0) ! new Tuple[Int](8)


      // COLLECT -> WAIT
      schedulerActors(0) ! new Tuple[Int](9)

    }
  }

}

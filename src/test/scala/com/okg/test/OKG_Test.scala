package com.okg.test

import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.okg.actor.{CoordinatorActor, InstanceActor, SchedulerActor}
import com.okg.state._
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

      val firstSchedulerStateProbe = TestProbe()
      schedulerActors(0) ! new SubscribeTransitionCallBack(firstSchedulerStateProbe.ref)
      firstSchedulerStateProbe.expectMsg(new CurrentState(schedulerActors(0), HASH))

      val secondSchedulerStateProbe = TestProbe()
      schedulerActors(1) ! new SubscribeTransitionCallBack(secondSchedulerStateProbe.ref)
      secondSchedulerStateProbe.expectMsg(new CurrentState(schedulerActors(1), HASH))

      val coordinatorStateProbe = TestProbe()
      coordinatorActorRef ! new SubscribeTransitionCallBack(coordinatorStateProbe.ref)
      coordinatorStateProbe.expectMsg(new CurrentState(coordinatorActorRef, WAIT_ALL))


      schedulerActors(0) ! new Tuple[Int](1)
      schedulerActors(0) ! new Tuple[Int](2)
      schedulerActors(0) ! new Tuple[Int](3)

      schedulerActors(1) ! new Tuple[Int](1)
      schedulerActors(1) ! new Tuple[Int](2)
      schedulerActors(1) ! new Tuple[Int](3)

      // HASH -> COLLECT

      schedulerActors(0) ! new Tuple[Int](4)
      schedulerActors(0) ! new Tuple[Int](5)
      schedulerActors(0) ! new Tuple[Int](6)
      schedulerActors(0) ! new Tuple[Int](7)
      schedulerActors(0) ! new Tuple[Int](8)

      schedulerActors(1) ! new Tuple[Int](4)
      schedulerActors(1) ! new Tuple[Int](5)
      schedulerActors(1) ! new Tuple[Int](6)
      schedulerActors(1) ! new Tuple[Int](7)
      schedulerActors(1) ! new Tuple[Int](8)

      firstSchedulerStateProbe.expectMsg(new Transition(schedulerActors(0), HASH, COLLECT))
      firstSchedulerStateProbe.expectMsg(new Transition(schedulerActors(0), COLLECT, WAIT))

      secondSchedulerStateProbe.expectMsg(new Transition(schedulerActors(1), HASH, COLLECT))
      secondSchedulerStateProbe.expectMsg(new Transition(schedulerActors(1), COLLECT, WAIT))

      coordinatorStateProbe.expectMsg(new Transition(coordinatorActorRef, WAIT_ALL, GENERATION))

      // COLLECT -> WAIT
      schedulerActors(0) ! new Tuple[Int](9)

    }
  }

}

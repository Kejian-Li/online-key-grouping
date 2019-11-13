package com.akka.inventory

import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.actor.{ActorSystem, Props}

class Main {

  def main(args: Array[String]): Unit = {
    val system = ActorSystem()
    val publisher = system.actorOf(Props(new Publisher(2, 2)))
    val inventory = system.actorOf(Props(new Inventory(publisher)))

    val stateProbe = new TestProbe(system)
    inventory ! new SubscribeTransitionCallBack(stateProbe.ref)
    stateProbe.expectMsg(new CurrentState(inventory, WaitForRequest))

    val replyProbe = TestProbe
    inventory ! new BookRequest("context 2", replyProbe.ref)
    stateProbe.expectMsg(new Transition(inventory, WaitForRequest, ProcessRequest))
    stateProbe.expectMsg(new Transition(inventory, ProcessRequest, WaitForRequest))

    replyProbe.expectMsg(new BookReply("context 2", Right(2)))


  }

}

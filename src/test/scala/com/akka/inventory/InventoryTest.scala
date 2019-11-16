package com.akka.inventory

import akka.actor.FSM.{CurrentState, SubscribeTransitionCallBack, Transition}
import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.akka.inventory.events.BookRequest
import com.akka.inventory.response.BookReply
import com.akka.inventory.state._
import org.scalatest.WordSpecLike

class InventoryTest extends TestKit(ActorSystem("InventoryTest"))
  with WordSpecLike {

  "Inventory" must {

    "follow the flow" in {
      val publisher = system.actorOf(Props(new Publisher(2, 2)))

      val inventory = system.actorOf(Props(new Inventory(publisher)))

      val stateProbe = TestProbe()

      val replyProbe = TestProbe()

      inventory ! new SubscribeTransitionCallBack(stateProbe.ref)

      stateProbe.expectMsg(new CurrentState(inventory, WaitForRequests))

      // start test
      inventory ! BookRequest("context1", replyProbe.ref)
      stateProbe.expectMsg(new Transition(inventory, WaitForRequests, WaitForPublisher))
      stateProbe.expectMsg(new Transition(inventory, WaitForPublisher, ProcessRequest))
      stateProbe.expectMsg(new Transition(inventory, ProcessRequest, WaitForRequests))
      replyProbe.expectMsg(new BookReply("context1", Right(1)))

      inventory ! BookRequest("context2", replyProbe.ref)
      stateProbe.expectMsg(new Transition(inventory, WaitForRequests, ProcessRequest))
      stateProbe.expectMsg(new Transition(inventory, ProcessRequest, WaitForRequests))
      replyProbe.expectMsg(new BookReply("context2", Right(2)))

      inventory ! BookRequest("context3", replyProbe.ref)
      stateProbe.expectMsg(new Transition(inventory, WaitForRequests, WaitForPublisher))
      stateProbe.expectMsg(new Transition(inventory, WaitForPublisher, ProcessSoldOut))
      replyProbe.expectMsg(new BookReply("context3", Left("Sold out")))
      stateProbe.expectMsg(new Transition(inventory, ProcessSoldOut, SoldOut))

      inventory ! BookRequest("context4", replyProbe.ref)
      stateProbe.expectMsg(new Transition(inventory, SoldOut, ProcessSoldOut))
      replyProbe.expectMsg(new BookReply("context4", Left("Sold out")))
      stateProbe.expectMsg(new Transition(inventory, ProcessSoldOut, SoldOut))

      system.stop(inventory)
      system.stop(publisher)
    }

  }
}

package com.akka.inventory

import akka.actor.ActorRef

case class BookRequest(context: String, target: ActorRef) {

}

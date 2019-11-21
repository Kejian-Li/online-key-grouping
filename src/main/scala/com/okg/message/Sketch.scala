package com.okg.message

import akka.dispatch.ControlMessage

import scala.collection.mutable

/**
  * Message inherited from {@Link ControlMessage} can be inserted into the head of mail-box's queue
  *
  * @param heavyHitters
  * @param buckets
  */
case class Sketch(heavyHitters: mutable.Map[Int, Int], buckets: Array[Int]) extends ControlMessage

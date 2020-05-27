package com.okg.wikipedia.message

import akka.dispatch.ControlMessage

import scala.collection.mutable

/**
  * Message inherited from {@Link ControlMessage} can be inserted into the head of mail-box's queue
  *
  * @param index index of Scheduler that sends this sketch
  * @param heavyHitters
  * @param buckets
  */
case class Sketch(index: Int,
                  heavyHitters: mutable.Map[String, Int],
                  buckets: Array[Int]) extends ControlMessage

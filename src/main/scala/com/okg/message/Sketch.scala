package com.okg.message

import scala.collection.mutable

case class Sketch(heavyHitters: mutable.Map[Int, Int], buckets: Array[Int]) extends Message

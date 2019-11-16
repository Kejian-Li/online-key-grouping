package com.okg.message

import scala.collection.mutable

case class Sketch(map: mutable.Map[Int, Int], A: Array[Int]) extends Message

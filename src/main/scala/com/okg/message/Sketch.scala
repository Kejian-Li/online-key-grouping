package com.okg.message

import scala.collection.mutable

case class Sketch(map: mutable.Map[Int, Int], array: Array[Int]) extends Message

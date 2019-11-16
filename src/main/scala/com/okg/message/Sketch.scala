package com.okg.message

import scala.collection.mutable

case class Sketch(map: mutable.Map[Integer, Integer], A: Array[Int]) extends Message


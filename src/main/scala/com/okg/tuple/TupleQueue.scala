package com.okg.tuple

import scala.collection.mutable

class TupleQueue[T] extends mutable.ArrayDeque[T] {

  def add(tuple: T) = {
    addOne(tuple)
  }
}

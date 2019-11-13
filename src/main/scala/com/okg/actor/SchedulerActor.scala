package com.okg.actor

import akka.actor.{Actor, FSM}
import com.okg.message._
import com.okg.state._
import com.okg.tuple.{Tuple, TupleQueue}
import com.okg.util.{SpaceSaving, TwoUniversalHash}

import scala.collection.JavaConverters

class SchedulerActor extends Actor with FSM[SchedulerState, MessageQueue[Message]] {

  private val N = 1000
  private val m = 100000
  private var n = 0

  private val k = 5
  private var spaceSaving: SpaceSaving = new SpaceSaving(epsilon, theta)
  private val A = new Array[Int](k)
  var hashFunction: TwoUniversalHash

  private var routingTable: RoutingTable = _

  private val epsilon = 0.05
  private val theta = 0.1

  private val messageQueue = new MessageQueue[Message]

  private var firstTupleQueue = new TupleQueue[Tuple[Int]]
  private var secondTupleQueue = new TupleQueue[Tuple[Int]]

  startWith(HASH, messageQueue)
  initializeHashFunction()


  private def initializeHashFunction() {
    val codomain = Math.ceil(k).toInt

    val uniformGenerator = new RandomDataGenerator()
    uniformGenerator.reSeed(1000)

    val prime = 10000019L
    val a = uniformGenerator.nextLong(1, prime - 1)
    val b = uniformGenerator.nextLong(1, prime - 1)
    hashFunction = new TwoUniversalHash(codomain, prime, a, b)
  }

  def hash(key: Int): Int = {
    hashFunction.hash(key)
  }

  def assignTuple(tuple: Tuple[Int]) = {

    val key = tuple.key
    if (routingTable.containsKey(key)) {
      routingTable.get(key)
    } else {
      hash(key)
    }
  }

  when(HASH) {
    case Event(tuple: Tuple[Int], _) => {
    n += 1
    assignTuple(tuple)

    if (n == N) {
      n = 0;    // reuse
      goto(COLLECT)
    }
  }

  when(COLLECT) {
    case Event(tuple: Tuple[Int], _) => {
    n += 1
    firstTupleQueue.add(tuple)
      val key = tuple.key

    spaceSaving.newSample(key)
    val index = hash(key)
      A.update(index, A.apply(index) + 1)

    if (n == m) {
      makeAndSendSketch(A)
      // clear
      n = 0
      for (i <- 1 to k) {
        A.update(i, 0)
      }
      spaceSaving = new SpaceSaving(epsilon, theta)

      goto(WAIT)
    }

  }

  when(WAIT) {
    case Event(tuple: Tuple[Int], tupleQueue: TupleQueue[Tuple[Int]]) => {
      n += 1
      secondTupleQueue.add(tuple)
      val key = tuple.key
      spaceSaving.newSample(key)
      val index = hash(key)
      A.update(index, A.apply(index) + 1)
    }

    case Event(routingTable: RoutingTable, _) => {
      this.routingTable = routingTable
      goto(ASSIGN)
    }
  }


  def assign()= {
    var tuple = firstTupleQueue.head
    while (tuple != null) {
      assignTuple(tuple)
      tuple = firstTupleQueue.head
    }
  }

  when(ASSIGN) {
    case Event(_, _) => {
       assign()
    }

  }


  def makeAndSendSketch(A : Array[Int]) = {
    val heavyHitters = JavaConverters.mapAsScalaMapConverter(spaceSaving.getHeavyHitters()).asScala  // M
    heavyHitters.foreach(entry => {
      val key = entry._1
      val frequency = entry._2
      val index = hash(key)
      A.update(index, A.apply(index) - frequency)

      val sketch = new Sketch(heavyHitters, A)

      )
  }

}

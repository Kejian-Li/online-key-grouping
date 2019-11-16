package com.okg.actor

import java.util

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.state._
import com.okg.tuple.{Tuple, TupleQueue}
import com.okg.util.{SpaceSaving, TwoUniversalHash}
import org.apache.commons.math3.random.RandomDataGenerator

/**
  * Class for Scheduler instance
  */

import scala.collection.mutable

class SchedulerActor(N: Int,   // number of received tuples before entering COLLECT state
                     m: Int,   // number of received tuples
                     k: Int,   // number of operator instance
                     epsilon: Double,
                     theta: Double,
                     coordinatorActor: ActorRef,
                     instanceActors: Array[ActorRef]) extends Actor with FSM[SchedulerState, SchedulerStateData] {

  val hashFunction = instantiateHashFunction()

  val schedulerStateDate = new SchedulerStateData(N,
    m,
    0,
    k,
    new SpaceSaving(epsilon, theta),
    new Sketch(mutable.Map.empty[Int, Int], new Array[Int](k)),
    new RoutingTable(mutable.Map.empty[Int, Int]),
    new TupleQueue[Tuple[Int]])

  startWith(HASH, schedulerStateDate)

  private def instantiateHashFunction() = {
    val codomain = Math.ceil(k).toInt

    val uniformGenerator = new RandomDataGenerator()
    uniformGenerator.reSeed(1000)

    val prime = 10000019L
    val a = uniformGenerator.nextLong(1, prime - 1)
    val b = uniformGenerator.nextLong(1, prime - 1)
    new TwoUniversalHash(codomain, prime, a, b)
  }

  def hash(key: Int): Int = {
    hashFunction.hash(key)
  }

  def assignTuple(tuple: Tuple[Int], routingTable: RoutingTable) = {
    var index = -1
    val key = tuple.key
    if (routingTable.containsKey(key)) {
      val value = routingTable.get(key)
      index = value.get
    } else {
      index = hash(key)
    }
    instanceActors(index) ! tuple
  }

  when(HASH) {
    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {
      schedulerStateData.copy(n = schedulerStateData.n + 1)
      assignTuple(tuple, schedulerStateData.routingTable)

      if (schedulerStateData.n == N) {
        goto(COLLECT) using (schedulerStateData.copy(n = 0))
      }
      stay()
    }
  }

  def updateSketch(heavyHitters: util.HashMap[Integer, Integer], sketch: Sketch) = {
    val it = heavyHitters.entrySet().iterator()

    while (it.hasNext) {
      val entry = it.next()
      sketch.map.put(entry.getKey, entry.getValue)
    }
  }

  when(COLLECT) {
    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {

      schedulerStateData.copy(n = schedulerStateData.n + 1)

      val key = tuple.key
      schedulerStateData.spaceSaving.newSample(key)
      val index = hash(key)
      schedulerStateData.sketch.A.update(index, schedulerStateData.sketch.A.apply(index) + 1)

      schedulerStateData.tupleQueue.addOne(tuple)

      if (schedulerStateData.n == m) {
        val heavyHitters = schedulerStateData.spaceSaving.getHeavyHitters
        updateSketch(heavyHitters, nextStateData.sketch)

        goto(WAIT)
      }
      stay()
    }
  }

  when(WAIT) {
    case Event(table: RoutingTable, schedulerStateData: SchedulerStateData) => {
      goto(ASSIGN) using (schedulerStateData.copy(routingTable = table))
    }
  }

  whenUnhandled {
    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {
      schedulerStateData.copy(n = schedulerStateData.n + 1)

      val key = tuple.key
      schedulerStateData.spaceSaving.newSample(key)
      val index = hash(key)
      schedulerStateData.sketch.A.update(index, schedulerStateData.sketch.A.apply(index) + 1)
      schedulerStateData.tupleQueue.addOne(tuple)

      stay()
    }
  }

  def assign(tupleQueue: TupleQueue[Tuple[Int]], routingTable: RoutingTable) = {
    var tuple = tupleQueue.head
    while (tuple != null) {
      assignTuple(tuple, routingTable)
      tuple = tupleQueue.head
    }
  }

  when(ASSIGN) {
    case Event(Done, schedulerStateData: SchedulerStateData) => {
      goto(COLLECT)
    }
  }

  onTransition {
    case _ -> WAIT => {
      // clear
      nextStateData.copy(n = 0)
      for (i <- 1 to k) {
        nextStateData.sketch.A.update(i, 0)
      }
      nextStateData.sketch.map.clear()
      nextStateData.copy(spaceSaving = new SpaceSaving(epsilon, theta))

      coordinatorActor ! nextStateData.sketch
    }

    case _ -> ASSIGN => {
      assign(nextStateData.tupleQueue, nextStateData.routingTable)
      self ! Done // assignment is completed
    }
  }

}



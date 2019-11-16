package com.okg.actor

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.state._
import com.okg.tuple.{Tuple, TupleQueue}
import com.okg.util.{SpaceSaving, TwoUniversalHash}

import scala.collection.{JavaConverters, mutable}

class SchedulerActor(N: Int,
                     m: Int,
                     k: Int,
                     epsilon: Double,
                     theta: Double,
                     coordinatorActor: ActorRef) extends Actor with FSM[SchedulerState, SchedulerStateData] {

  val hashFunction = instantiateHashFunction()

  startWith(HASH, new SchedulerStateData(N,
    m,
    0,
    k,
    new SpaceSaving(epsilon, theta),
    new Array[Int](k),
    new RoutingTable(mutable.Map.empty[Int, Int]),
    new TupleQueue[Tuple[Int]]),
    new Sketch(mutable.Map.empty[Integer, Integer], new Array[Int](k))

  )


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

  def assignTuple(tuple: Tuple[Int], routingTable: RoutingTable): Int = {
    val key = tuple.key
    if (routingTable.containsKey(key)) {
      val value = routingTable.get(key)
      value.get
    } else {
      hash(key)
    }
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

  when(COLLECT) {
    case Event(Done, schedulerStateData: SchedulerStateData) => {
      goto(WAIT)
    }
  }

  when(WAIT) {
    case Event(routingTable: RoutingTable, schedulerStateData: SchedulerStateData) => {
      goto(ASSIGN) using (schedulerStateData.copy(routingTable = routingTable))
    }
  }

  whenUnhandled {
    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {

      val key = tuple.key
      schedulerStateData.spaceSaving.newSample(key)
      val index = hash(key)
      schedulerStateData.A.update(index, schedulerStateData.A.apply(index) + 1)

      if (schedulerStateData.n == m) {
        makeAndSendSketch(schedulerStateData.A, schedulerStateData.spaceSaving)
        // clear
        schedulerStateData.copy(n = 0)
        for (i <- 1 to k) {
          schedulerStateData.A.update(i, 0)
        }

        goto(COLLECT) using (schedulerStateData.copy(spaceSaving = new SpaceSaving(epsilon, theta)))
      }
      stay() using (schedulerStateData.copy(n = schedulerStateData.n + 1,
        tupleQueue = schedulerStateData.tupleQueue.addOne(tuple)))
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
    case _ -> COLLECT => {
      self ! Done // collect is completed
    }
    case _ -> ASSIGN => {
      assign(nextStateData.tupleQueue, nextStateData.routingTable)
      self ! Done // assignment is completed
    }
    case _ -> WAIT => {
      coordinatorActor ! sketch
    }
  }

  def makeAndSendSketch(A: Array[Int], spaceSaving: SpaceSaving) = {
    val heavyHitters = JavaConverters.mapAsScalaMapConverter(spaceSaving.getHeavyHitters())
      .asScala.toMap // M

    heavyHitters.foreach(entry => {
      val key = entry._1
      val frequency = entry._2
      val index = hash(key)
      A.update(index, A.apply(index) - frequency)

      sketch = new Sketch(heavyHitters, A)
    }
    )
  }

}



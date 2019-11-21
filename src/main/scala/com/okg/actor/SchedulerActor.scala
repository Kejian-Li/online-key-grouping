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

class SchedulerActor(index: Int, // index of this Scheduler instance
                     N: Int, // number of received tuples before entering COLLECT state
                     m: Int, // number of involved tuples in each period
                     k: Int, // number of Operator instances
                     epsilon: Double,
                     theta: Double,
                     coordinatorActor: ActorRef,
                     instanceActors: Array[ActorRef]) extends Actor with FSM[SchedulerState, SchedulerStateData] {

  val hashFunction = instantiateHashFunction()

  val schedulerStateDate = new SchedulerStateData(N,
    m,
    k,
    new SpaceSaving(epsilon, theta),
    new Sketch(mutable.Map.empty[Int, Int], new Array[Int](k)),
    new RoutingTable(mutable.Map.empty[Int, Int]),
    new TupleQueue[Tuple[Int]])

  startWith(HASH, schedulerStateDate)

  private def instantiateHashFunction() = {

    val uniformGenerator = new RandomDataGenerator()
    uniformGenerator.reSeed(1000)

    val prime = 10000019L
    val a = uniformGenerator.nextLong(1, prime - 1)
    val b = uniformGenerator.nextLong(1, prime - 1)
    new TwoUniversalHash(k, prime, a, b)
  }

  def hash(key: Int): Int = {
    hashFunction.hash(key)
  }

  def assignTuple(tuple: Tuple[Int], routingTable: RoutingTable) = {
    var targetIndex = -1
    val key = tuple.key
    if (routingTable.contains(key)) {
      targetIndex = routingTable.get(key)
    } else {
      targetIndex = hash(key)
    }
    log.info("Scheduler " + index + " assigns tuple to target Operator instance " + targetIndex)
    instanceActors(targetIndex) ! tuple
  }

  var n = 0

  when(HASH) {
    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {
      n += 1
      assignTuple(tuple, schedulerStateData.routingTable)

      if (n == N) {
        log.info("Scheduler " + index + " received " + n + " tuples in total at HASH state")
        log.info("Scheduler " + index + " is gonna LEARN state")
        goto(LEARN)
      } else {
        stay()
      }
    }
  }

  // from java's HashMap to Scala's mutable.Map
  def putHeavyHittersIntoSketch(rawHeavyHitters: util.HashMap[Integer, Integer], sketch: Sketch) = {
    val it = rawHeavyHitters.entrySet().iterator()

    while (it.hasNext) {
      val entry = it.next()
      sketch.heavyHitters.put(entry.getKey, entry.getValue)
    }
  }

  var period = 1

  when(LEARN) {
    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {
      val tupleQueue = schedulerStateData.tupleQueue
      tupleQueue += tuple

      if (tupleQueue.size >= m) {
        log.info("Scheduler " + index + " enters " + period + " period")
        period += 1
        // learn
        for (i <- 1 to m) {
          val tuple = tupleQueue.head // return first tuple but don't remove
          val key = tuple.key
          schedulerStateData.spaceSaving.newSample(key)
          val targetIndex = hash(key)
          schedulerStateData.sketch.buckets.update(targetIndex, schedulerStateData.sketch.buckets(targetIndex) + 1)
        }
        // make and send sketch
        val rawHeavyHittersMap = schedulerStateData.spaceSaving.getHeavyHitters
        putHeavyHittersIntoSketch(rawHeavyHittersMap, schedulerStateData.sketch)
        coordinatorActor ! new Sketch(schedulerStateData.sketch.heavyHitters.clone(), schedulerStateData.sketch.buckets.clone())
        log.info("Scheduler " + index + " send sketch successfully")

        log.info("Scheduler " + index + " is gonna WAIT state")
        goto(WAIT) using (schedulerStateData.copy(spaceSaving = new SpaceSaving(epsilon, theta),
          sketch = new Sketch(mutable.Map.empty[Int, Int], new Array[Int](k))))
      } else {
        stay()
      }
    }

  }

  when(WAIT) {
    case Event(startAssignment: StartAssignment, schedulerStateData: SchedulerStateData) => {
      log.info("Scheduler " + index + " received routing table, starting assignment...")
      goto(ASSIGN) using (schedulerStateData.copy(routingTable = startAssignment.routingTable))
    }
  }

  whenUnhandled {
    case Event(StartSimulation, schedulerStateData: SchedulerStateData) => {
      coordinatorActor ! StartSimulation

      for (i <- 0 to k - 1) {
        instanceActors(i) ! StartSimulation
      }
      stay()
    }

    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {
      schedulerStateData.tupleQueue += tuple
      stay()
    }

    case Event(TerminateSimulation, schedulerStateData: SchedulerStateData) => {
      for (i <- 1 to instanceActors.size - 1) {
        instanceActors(i) forward (TerminateSimulation) // forward termination notification to instances
      }
      stay()
    }
  }

  def assign(tupleQueue: TupleQueue[Tuple[Int]], routingTable: RoutingTable) = {
    var x = 0
    while (x < m) {
      val tuple = tupleQueue.dequeue() // return and remove first element
      assignTuple(tuple, routingTable)
      x += 1
    }
  }

  when(ASSIGN) {
    case Event(AssignmentCompleted, schedulerStateData: SchedulerStateData) => {
      log.info("Scheduler " + index + " is gonna LEARN state")
      goto(LEARN)
    }
  }

  onTransition {
    case _ -> ASSIGN => {
      assign(nextStateData.tupleQueue, nextStateData.routingTable)
      self ! AssignmentCompleted // assignment in each period is completed
    }
  }

}



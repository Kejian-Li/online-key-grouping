package com.okg.actor

import java.util

import akka.actor.{Actor, ActorRef, FSM}
import com.okg.message._
import com.okg.message.communication.{AssignmentCompleted, StartSimulation, TerminateSimulation}
import com.okg.state._
import com.okg.tuple.{Tuple, TupleQueue}
import com.okg.util.{SpaceSaving, TwoUniversalHash}
import org.apache.commons.math3.random.RandomDataGenerator

/**
  * Class for Scheduler instance
  */

import scala.collection.mutable

class SchedulerActor(index: Int, // index of this Scheduler instance
                     m: Int, // number of involved tuples in each period
                     k: Int, // number of Operator instances
                     epsilon: Double,
                     theta: Double,
                     coordinatorActor: ActorRef,
                     instanceActors: Array[ActorRef]) extends Actor with FSM[SchedulerState, SchedulerStateData] {

  var hashFunction: TwoUniversalHash = null

  //initialize
  override def preStart(): Unit = {
    instantiateHashFunction()
  }

  startWith(LEARN, initializeSchedulerStateDate())

  private def initializeSchedulerStateDate() = {
    new SchedulerStateData(
      m,
      k,
      new SpaceSaving(epsilon, theta),
      new Sketch(mutable.Map.empty[Int, Int], new Array[Int](k)),
      new RoutingTable(mutable.Map.empty[Int, Int]),
      new TupleQueue[Tuple[Int]])
  }

  private def instantiateHashFunction() = {
    val uniformGenerator = new RandomDataGenerator()
    uniformGenerator.reSeed(1000)

    val prime = 10000019L
    val a = uniformGenerator.nextLong(1, prime - 1)
    val b = uniformGenerator.nextLong(1, prime - 1)
    hashFunction = new TwoUniversalHash(k, prime, a, b)
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
//    targetIndex = hash(key)
    instanceActors(targetIndex) ! tuple
  }

  // from java's HashMap to Scala's mutable.Map
  def putHeavyHittersIntoSketch(rawHeavyHitters: util.HashMap[Integer, Integer], sketch: Sketch) = {
    val it = rawHeavyHitters.entrySet().iterator()

    while (it.hasNext) {
      val entry = it.next()
      sketch.heavyHitters.put(entry.getKey, entry.getValue)
    }
  }

  var assignedTuplesNum = 0
  var period = 1

  when(LEARN) {
    case Event(tuple: Tuple[Int], schedulerStateData: SchedulerStateData) => {
      val tupleQueue = schedulerStateData.tupleQueue
      tupleQueue += tuple

      if (tupleQueue.size >= m) {
        log.info("Scheduler " + index + " enters " + period + " period")
        log.info("Scheduler " + index + " assigned so far " + assignedTuplesNum + " tuples in total")
        // learn
        var i = 0
        val it = tupleQueue.iterator
        while (i < m) {
          val tuple = it.next() // return but don't remove
          val key = tuple.key
          schedulerStateData.spaceSaving.newSample(key)
          val targetIndex = hash(key)
          schedulerStateData.sketch.buckets.update(targetIndex, schedulerStateData.sketch.buckets(targetIndex) + 1)
          i += 1
        }
        // make and send sketch
        val rawHeavyHittersMap = schedulerStateData.spaceSaving.getHeavyHitters
        putHeavyHittersIntoSketch(rawHeavyHittersMap, schedulerStateData.sketch)
        val sketch = new Sketch(schedulerStateData.sketch.heavyHitters.clone(), schedulerStateData.sketch.buckets.clone())
        coordinatorActor ! sketch

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
      for (i <- 0 to k - 1) {
        instanceActors(i) forward (TerminateSimulation) // forward termination notification to instances
      }
      stay()
    }
  }

  def assign(tupleQueue: TupleQueue[Tuple[Int]], routingTable: RoutingTable) = {
    var x = 0
    while (x < m) {
      assignedTuplesNum += 1
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
    case _ -> LEARN => {
      period += 1
    }
    case _ -> ASSIGN => {
      assign(nextStateData.tupleQueue, nextStateData.routingTable)
      self ! AssignmentCompleted // assignment in each period is completed
    }
  }

}



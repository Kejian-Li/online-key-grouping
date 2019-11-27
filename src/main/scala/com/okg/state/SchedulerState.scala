package com.okg.state

sealed trait SchedulerState extends State

case object LEARN extends SchedulerState

case object WAIT extends SchedulerState

case object ASSIGN extends SchedulerState

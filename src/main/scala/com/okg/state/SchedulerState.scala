package com.okg.state

sealde trait SchedulerState extends State

case object HASH extends SchedulerState

case object COLLECT extends SchedulerState

case object WAIT extends SchedulerState

case object ASSIGN extends SchedulerState

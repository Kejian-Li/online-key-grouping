package com.okg.wikipedia.state

sealed trait InstanceState extends State

case object RUN extends InstanceState

case object MIGRATE extends InstanceState

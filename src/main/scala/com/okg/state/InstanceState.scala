package com.okg.state

sealed trait InstanceState extends State

case object RUN extends InstanceState

case object MIGRATION extends InstanceState

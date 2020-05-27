package com.okg.wikipedia.message

import scala.collection.mutable

class MessageQueue[Message] extends mutable.Queue[Message]
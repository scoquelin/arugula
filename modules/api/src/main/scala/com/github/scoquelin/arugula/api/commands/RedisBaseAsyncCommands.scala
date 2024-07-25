package com.github.scoquelin.arugula.api.commands

import scala.concurrent.Future

trait RedisBaseAsyncCommands[K, V] {
  def ping: Future[String]
}

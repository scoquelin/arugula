package com.github.scoquelin.arugula.api.commands.client

import com.github.scoquelin.arugula.api.commands.RedisAsyncCommands

import scala.concurrent.Future

trait RedisCommandsClient[K, V] {
  def sendCommand[T](command: RedisAsyncCommands[K, V] => Future[T]): Future[T]
}

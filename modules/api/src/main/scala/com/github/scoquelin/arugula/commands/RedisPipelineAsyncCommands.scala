package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

import com.github.scoquelin.arugula.RedisCommandsClient

/**
 * Asynchronous pipeline (batch of async commands) implementation
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisPipelineAsyncCommands[K, V] {
  def pipeline(commands: RedisCommandsClient[K, V] => List[Future[Any]]): Future[Option[List[Any]]]
}

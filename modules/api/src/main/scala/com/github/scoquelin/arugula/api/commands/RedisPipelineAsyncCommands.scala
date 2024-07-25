package com.github.scoquelin.arugula.api.commands

import scala.concurrent.Future

/**
 * Asynchronous pipeline (batch of async commands) implementation
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisPipelineAsyncCommands[K, V] {
  def pipeline(commands: RedisAsyncCommands[K, V] => List[Future[Any]]): Future[Option[List[Any]]]
}

package com.github.scoquelin.arugula.api.commands

import scala.concurrent.Future

/**
 * Asynchronous commands for interacting with Redis Server
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisServerAsyncCommands[K, V] {
  def flushAll: Future[Unit]
  def info: Future[Map[String, String]]
}

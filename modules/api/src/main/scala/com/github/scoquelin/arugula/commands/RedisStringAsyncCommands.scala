package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Asynchronous commands for manipulating/querying Strings
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisStringAsyncCommands[K, V] {
  def get(key: K): Future[Option[V]]
  def set(key: K, value: V): Future[Unit]
  def setEx(key: K, value: V, expiresIn: FiniteDuration): Future[Unit]
  def setNx(key: K, value: V): Future[Boolean]
}

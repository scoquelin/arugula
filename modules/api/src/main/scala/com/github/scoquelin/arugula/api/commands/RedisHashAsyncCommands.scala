package com.github.scoquelin.arugula.api.commands

import scala.concurrent.Future

/**
 * Asynchronous commands for manipulating/querying Hashes (key/value pairs)
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisHashAsyncCommands[K, V] {
  def hDel(key: K, fields: K*): Future[Long]
  def hGet(key: K, field: K): Future[Option[V]]
  def hGetAll(key: K): Future[Map[K, V]]
  def hSet(key: K, field: K, value: V): Future[Boolean]
  def hMSet(key: K, values: Map[K, V]): Future[Unit]
  def hSetNx(key: K, field: K, value: V): Future[Boolean]
  def hIncrBy(key: K, field: K, amount: Long): Future[Long]
}

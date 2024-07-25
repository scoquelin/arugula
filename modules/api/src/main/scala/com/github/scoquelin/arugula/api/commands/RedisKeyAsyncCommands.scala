package com.github.scoquelin.arugula.api.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Asynchronous commands for manipulating/querying Keys
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisKeyAsyncCommands[K, V] {
  def del(key: K*): Future[Long]
  def exists(key: K*): Future[Boolean]
  def expire(key: K, expiresIn: FiniteDuration): Future[Boolean]
  def ttl(key: K): Future[Option[FiniteDuration]]
}

object RedisKeyAsyncCommands {
  final case class ScanCursor(cursor: String, finished: Boolean)

  object ScanCursor{
    def apply(cursor: String) = new ScanCursor(cursor, finished = false)

    val Initial: ScanCursor = ScanCursor("0", finished = false)

    val Finished: ScanCursor = ScanCursor("0", finished = true)
  }

}
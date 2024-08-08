package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

trait RedisListAsyncCommands[K, V] {
  def blMove(
    source: K,
    destination: K,
    sourceSide: RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Right,
    destinationSide: RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Left,
    timeout: Double = 0.0, // zero is infinite wait
  ): Future[Option[V]]
  def blMPop(
    keys: List[K],
    direction:RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Left,
    count: Int = 1,
    timeout: Double = 0.0,
  ): Future[Option[(K, List[V])]]
  def lMPop(
    keys: List[K],
    direction:RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Left,
    count: Int = 1,
  ): Future[Option[(K, List[V])]]
  def lPush(key: K, values: V*): Future[Long]
  def rPush(key: K, values: V*): Future[Long]
  def lPop(key: K): Future[Option[V]]
  def rPop(key: K): Future[Option[V]]
  def lRange(key: K, start: Long, end: Long): Future[List[V]]
  def lMove(
    source: K,
    destination: K,
    sourceSide: RedisListAsyncCommands.Side,
    destinationSide: RedisListAsyncCommands.Side
  ): Future[Option[V]]
  def lPos(key: K, value: V): Future[Option[Long]]
  def lLen(key: K): Future[Long]
  def lRem(key: K, count: Long, value: V): Future[Long]
  def lTrim(key: K, start: Long, end: Long): Future[Unit]
  def lIndex(key: K, index: Long): Future[Option[V]]
}

object RedisListAsyncCommands {
  sealed trait Side

  object Side {
    case object Left extends Side

    case object Right extends Side

  }
}

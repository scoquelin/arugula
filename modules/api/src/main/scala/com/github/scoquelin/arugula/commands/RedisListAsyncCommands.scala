package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

trait RedisListAsyncCommands[K, V] {
  def lPush(key: K, values: V*): Future[Long]
  def rPush(key: K, values: V*): Future[Long]
  def lPop(key: K): Future[Option[V]]
  def rPop(key: K): Future[Option[V]]
  def lRange(key: K, start: Long, end: Long): Future[List[V]]
  def lPos(key: K, value: V): Future[Option[Long]]
  def lLen(key: K): Future[Long]
  def lRem(key: K, count: Long, value: V): Future[Long]
  def lTrim(key: K, start: Long, end: Long): Future[Unit]
  def lIndex(key: K, index: Long): Future[Option[V]]
}


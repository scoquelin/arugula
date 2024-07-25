package com.github.scoquelin.arugula.connection

import scala.concurrent.Future

import io.lettuce.core.api.async.{RedisAsyncCommands => JRedisAsyncCommands}
import io.lettuce.core.cluster.api.async.{RedisClusterAsyncCommands => JRedisClusterAsyncCommands}

private[arugula] trait RedisConnection[K, V] {
  def async: Future[JRedisAsyncCommands[K, V]]

  def clusterAsync: Future[JRedisClusterAsyncCommands[K, V]]

  def setAutoFlushCommands(autoFlush: Boolean): Future[Unit]
  def flushCommands: Future[Unit]

  def close: Future[Unit]
}


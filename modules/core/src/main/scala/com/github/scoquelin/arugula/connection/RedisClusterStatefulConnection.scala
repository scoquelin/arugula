package com.github.scoquelin.arugula.connection

import scala.concurrent.{ExecutionContext, Future, blocking}

import io.lettuce.core.api.async.{RedisAsyncCommands => JRedisAsyncCommands}
import io.lettuce.core.cluster.api.async.{RedisClusterAsyncCommands => JRedisClusterAsyncCommands}
import io.lettuce.core.cluster.api.{StatefulRedisClusterConnection => JStatefulRedisClusterConnection}

private[arugula] class RedisClusterStatefulConnection[K, V](conn: Future[JStatefulRedisClusterConnection[K, V]])(implicit ec: ExecutionContext) extends RedisConnection[K, V] {
  override def async: Future[JRedisAsyncCommands[K, V]] =
    Future.failed(new IllegalStateException("Transactions are not supported in a cluster. Cluster commands are available through clusterAsync() method."))

  override def clusterAsync: Future[JRedisClusterAsyncCommands[K, V]] = conn.map(_.async())

  override def setAutoFlushCommands(autoFlush: Boolean): Future[Unit] = conn.map(_.setAutoFlushCommands(autoFlush))
  override def flushCommands: Future[Unit] = conn.flatMap(connection => Future(blocking(connection.flushCommands())))

  override def close: Future[Unit] = conn.map(_.close())

}

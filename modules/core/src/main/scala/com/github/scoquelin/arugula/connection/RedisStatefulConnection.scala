package com.github.scoquelin.arugula.connection

import scala.concurrent.{ExecutionContext, Future, blocking}

import io.lettuce.core.api.async.{RedisAsyncCommands => JRedisAsyncCommands}
import io.lettuce.core.api.{StatefulRedisConnection => JStatefulRedisConnection}

private[arugula] class RedisStatefulConnection[K, V] (conn: Future[JStatefulRedisConnection[K, V]])(implicit ec: ExecutionContext) extends RedisConnection[K, V] {
    override def async: Future[JRedisAsyncCommands[K, V]] = conn.map(_.async())

    override def clusterAsync: Future[JRedisAsyncCommands[K, V]] =
        Future.failed(new IllegalStateException("Running in a single node so cluster commands are not available"))

    override def setAutoFlushCommands(autoFlush: Boolean): Future[Unit] = conn.map(_.setAutoFlushCommands(autoFlush))
    override def flushCommands: Future[Unit] = conn.flatMap(connection => Future(blocking(connection.flushCommands())))

    override def close: Future[Unit] = conn.map(_.close())
}

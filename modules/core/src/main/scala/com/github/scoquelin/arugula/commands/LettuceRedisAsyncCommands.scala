package com.github.scoquelin.arugula.commands

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._

import com.github.scoquelin.arugula.api.commands.RedisAsyncCommands
import com.github.scoquelin.arugula.commands.internal._
import com.github.scoquelin.arugula.connection.RedisConnection
import io.lettuce.core.RedisFuture
import io.lettuce.core.cluster.api.async.{RedisClusterAsyncCommands => JRedisClusterAsyncCommands}

/**
 * This is the Lettuce implementation of the RedisAsyncCommands API.
 * This implementation is meant to evolve and any Redis operation that is supported by the Lettuce API could be potentially added here.
 *
 * @param connection The underlying Lettuce connection that will be used to relay commands.
 * @param cluster Flag explicitly conveying if we are if a cluster-enabled environment(client/server) or not
 * @param executionContext The execution context
 * @tparam K type of the key
 * @tparam V type of the value
 */
private[commands] class LettuceRedisAsyncCommands[K, V](
  connection: RedisConnection[K, V],
  cluster: Boolean,
  )(override private[commands] implicit val executionContext: ExecutionContext)
  extends RedisAsyncCommands[K, V]
    with LettuceRedisCommandDelegation[K, V]
    with LettuceRedisBaseAsyncCommands[K, V]
    with LettuceRedisKeyAsyncCommands[K, V]
    with LettuceRedisHashAsyncCommands[K, V]
    with LettuceRedisServerAsyncCommands[K, V]
    with LettuceRedisListAsyncCommands[K, V]
    with LettuceRedisStringAsyncCommands[K, V]
    with LettuceRedisSortedSetAsyncCommands[K, V]
    with LettuceRedisPipelineAsyncCommands[K, V] {


  //Generalizing over JRedisClusterAsyncCommands for both single-node and cluster commands as we don't need transaction commands (that are single-node only)
  //(relying on the fact that io.lettuce.core.api.async.RedisAsyncCommands also extends io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands)
  private val lettuceRedisClusterAsyncCommands: Future[JRedisClusterAsyncCommands[K, V]] =
    if (cluster) connection.clusterAsync else connection.async


  /**
   * Lifts the outcome (RedisFuture[T]) of a Lettuce Redis command into a Scala Future[T]
   *
   * @param command The Lettuce command to be dispatched
   * @tparam T The type of the returned value inside the io.lettuce.core.RedisFuture[T]
   * @return A Scala Future[T] extracted from the io.lettuce.core.RedisFuture[T]
   */
  override private[commands] def delegateRedisClusterCommandAndLift[T](command: JRedisClusterAsyncCommands[K, V] => RedisFuture[T]): Future[T] =
    lettuceRedisClusterAsyncCommands.flatMap(redisCommands => command(redisCommands).toCompletableFuture.asScala)


}

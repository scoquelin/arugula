package com.github.scoquelin.arugula.commands.internal

import scala.concurrent.{ExecutionContext, Future}

/**
 * A trait that provides a delegation mechanism for Lettuce Redis commands
 * to be dispatched and lifted into Scala Futures from the Lettuce RedisFuture
 *
 * @tparam K The type of the Redis key
 * @tparam V The type of the Redis value
 */
private[commands] trait LettuceRedisCommandDelegation[K, V] {
  /**
   * Lifts the outcome (RedisFuture[T]) of a Lettuce Redis command into a Scala Future[T]
   *
   * @param command The Lettuce command to be dispatched
   * @tparam T The type of the returned value inside the io.lettuce.core.RedisFuture[T]
   * @return A Scala Future[T] extracted from the io.lettuce.core.RedisFuture[T]
   */
  private[commands] def delegateRedisClusterCommandAndLift[T](
    command: io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands[K, V] => io.lettuce.core.RedisFuture[T]
  ): Future[T]

  /**
   * The execution context
   * Should be injected by the implementing class
   */
  private[commands] implicit val executionContext: ExecutionContext
}

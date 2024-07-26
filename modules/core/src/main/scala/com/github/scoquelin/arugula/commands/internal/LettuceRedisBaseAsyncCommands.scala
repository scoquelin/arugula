package com.github.scoquelin.arugula.commands.internal

import scala.concurrent.Future

import com.github.scoquelin.arugula.api.commands.RedisBaseAsyncCommands

private[commands] trait LettuceRedisBaseAsyncCommands[K, V] extends RedisBaseAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {
  override def ping: Future[String] = delegateRedisClusterCommandAndLift(_.ping())
}

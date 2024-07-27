package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

private[arugula] trait LettuceRedisBaseAsyncCommands[K, V] extends RedisBaseAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {
  override def ping: Future[String] = delegateRedisClusterCommandAndLift(_.ping())
}

package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

trait LettuceRedisHLLAsyncCommands[K, V] extends RedisHLLAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {
  override def pfAdd(key: K, values: V*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.pfadd(key, values: _*)).map(Long2long)

  override def pfMerge(destinationKey: K, sourceKeys: K*): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.pfmerge(destinationKey, sourceKeys: _*)).map(_ => ())

  override def pfCount(keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.pfcount(keys: _*)).map(Long2long)
}

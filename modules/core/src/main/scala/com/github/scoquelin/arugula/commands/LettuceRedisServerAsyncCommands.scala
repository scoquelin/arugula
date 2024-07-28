package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation


private[arugula] trait LettuceRedisServerAsyncCommands[K, V] extends RedisServerAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {

  override def flushAll: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.flushall()).map(_ => ())

  override def info: Future[Map[String, String]] = {
    def parseInfo(info: String): Map[String, String] =
      info
        .split("\\r?\\n")
        .toList
        .map(_.split(":", 2).toList)
        .collect { case k :: v :: Nil => (k, v) }
        .toMap

    delegateRedisClusterCommandAndLift(_.info()).map(parseInfo)
  }
}

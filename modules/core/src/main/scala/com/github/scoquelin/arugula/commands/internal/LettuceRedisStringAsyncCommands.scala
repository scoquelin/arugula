package com.github.scoquelin.arugula.commands.internal

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import com.github.scoquelin.arugula.api.commands.RedisStringAsyncCommands

import java.util.concurrent.TimeUnit

private[commands] trait LettuceRedisStringAsyncCommands[K, V] extends RedisStringAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {
  override def get(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.get(key)).map(Option.apply)

  override def set(key: K, value: V): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.set(key, value)).map(_ => ())

  override def setEx(key: K, value: V, expiresIn: FiniteDuration): Future[Unit] =
    (expiresIn.unit match {
      case TimeUnit.MILLISECONDS | TimeUnit.MICROSECONDS | TimeUnit.NANOSECONDS =>
        delegateRedisClusterCommandAndLift(_.psetex(key, expiresIn.toMillis, value))
      case _ =>
        delegateRedisClusterCommandAndLift(_.setex(key, expiresIn.toSeconds, value))
    }).map(_ => ())

  override def setNx(key: K, value: V): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.setnx(key, value)).map(Boolean2boolean)
}

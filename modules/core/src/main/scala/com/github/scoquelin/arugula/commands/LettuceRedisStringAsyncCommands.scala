package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

import java.util.concurrent.TimeUnit

private[arugula] trait LettuceRedisStringAsyncCommands[K, V] extends RedisStringAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {
  override def get(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.get(key)).map(Option.apply)

  override def getDel(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.getdel(key)).map(Option.apply)

  override def getSet(key: K, value: V): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.getset(key, value)).map(Option.apply)

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

  override def incr(key: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.incr(key)).map(Long2long)

  override def incrBy(key: K, increment: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.incrby(key, increment)).map(Long2long)

  override def incrByFloat(key: K, increment: Double): Future[Double] =
    delegateRedisClusterCommandAndLift(_.incrbyfloat(key, increment)).map(Double2double)

  override def decr(key: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.decr(key)).map(Long2long)

  override def decrBy(key: K, decrement: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.decrby(key, decrement)).map(Long2long)
}

package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

import java.util.concurrent.TimeUnit

private[arugula] trait LettuceRedisKeyAsyncCommands[K, V] extends RedisKeyAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {
  import LettuceRedisKeyAsyncCommands.toFiniteDuration

  override def del(key: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.del(key: _*)).map(Long2long)

  override def exists(key: K*): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.exists(key: _*)).map(_ == key.size.toLong)

  override def expire(key: K, expiresIn: FiniteDuration): Future[Boolean] =
    (expiresIn.unit match {
      case TimeUnit.MILLISECONDS | TimeUnit.MICROSECONDS | TimeUnit.NANOSECONDS =>
        delegateRedisClusterCommandAndLift(_.pexpire(key, expiresIn.toMillis))
      case _ =>
        delegateRedisClusterCommandAndLift(_.expire(key, expiresIn.toSeconds))
    }).map(Boolean2boolean)

  override def ttl(key: K): Future[Option[FiniteDuration]] =
    delegateRedisClusterCommandAndLift(_.ttl(key)).map(toFiniteDuration(TimeUnit.SECONDS))
}

private[this] object LettuceRedisKeyAsyncCommands {
  private[commands] def toFiniteDuration(units: TimeUnit)(duration: java.lang.Long): Option[FiniteDuration] =
    duration match {
      case d if d < 0 => None
      case d => Some(FiniteDuration(d, units))
    }
}
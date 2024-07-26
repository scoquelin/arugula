package com.github.scoquelin.arugula.commands.internal

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.api.commands.RedisListAsyncCommands

private[commands] trait LettuceRedisListAsyncCommands[K, V] extends RedisListAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V]{

  override def lRem(key: K, count: Long, value: V): Future[Long] =
    delegateRedisClusterCommandAndLift(_.lrem(key, count, value)).map(Long2long)

  override def lTrim(key: K, start: Long, stop: Long): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.ltrim(key, start, stop)).map(_ => ())

  override def lRange(key: K, start: Long, stop: Long): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.lrange(key, start, stop)).map(_.asScala.toList)

  override def lPos(key: K, value: V): Future[Option[Long]] =
    delegateRedisClusterCommandAndLift(_.lpos(key, value)).map(Option(_).map(Long2long))

  override def lLen(key: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.llen(key)).map(Long2long)

  override def lPop(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.lpop(key)).map(Option.apply)

  override def lPush(key: K, values: V*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.lpush(key, values: _*)).map(Long2long)

  override def rPop(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.rpop(key)).map(Option.apply)

  override def rPush(key: K, values: V*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.rpush(key, values: _*)).map(Long2long)

  override def lIndex(key: K, index: Long): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.lindex(key, index)).map(Option.apply)

}

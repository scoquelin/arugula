package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

private[arugula] trait LettuceRedisHashAsyncCommands[K, V] extends RedisHashAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V]{

  override def hDel(key: K, fields: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.hdel(key, fields: _*)).map(Long2long)

  override def hGet(key: K, field: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.hget(key, field)).map(Option.apply)

  override def hGetAll(key: K): Future[Map[K, V]] =
    delegateRedisClusterCommandAndLift(_.hgetall(key)).map(_.asScala.toMap)

  override def hIncrBy(key: K, field: K, amount: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.hincrby(key, field, amount)).map(Long2long)

  override def hSet(key: K, field: K, value: V): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.hset(key, field, value)).map(Boolean2boolean)

  override def hMSet(key: K, values: Map[K, V]): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.hmset(key, values.asJava)).map(_ => ())

  override def hSetNx(key: K, field: K, value: V): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.hsetnx(key, field, value)).map(Boolean2boolean)

}
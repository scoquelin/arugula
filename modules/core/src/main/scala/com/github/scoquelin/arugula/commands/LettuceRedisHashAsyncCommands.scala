package com.github.scoquelin.arugula.commands

import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation
import io.lettuce.core.ScanArgs

private[arugula] trait LettuceRedisHashAsyncCommands[K, V] extends RedisHashAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V]{

  override def hDel(key: K, fields: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.hdel(key, fields: _*)).map(Long2long)

  override def hExists(key: K, field: K): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.hexists(key, field)).map(Boolean2boolean)

  override def hGet(key: K, field: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.hget(key, field)).map(Option.apply)

  override def hGetAll(key: K): Future[Map[K, V]] =
    delegateRedisClusterCommandAndLift(_.hgetall(key)).map(_.asScala.toMap)

  override def hIncrBy(key: K, field: K, amount: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.hincrby(key, field, amount)).map(Long2long)

  override def hRandField(key: K): Future[Option[K]] =
    delegateRedisClusterCommandAndLift(_.hrandfield(key)).map(Option.apply)

  override def hRandField(key: K, count: Long): Future[List[K]] =
    delegateRedisClusterCommandAndLift(_.hrandfield(key, count)).map(_.asScala.toList)

  override def hRandFieldWithValues(key: K): Future[Option[(K, V)]] =
    delegateRedisClusterCommandAndLift(_.hrandfieldWithvalues(key)).map{ kv =>
      if(kv.hasValue) Some(kv.getKey -> kv.getValue) else None
    }

  override def hRandFieldWithValues(key: K, count: Long): Future[Map[K, V]] =
    delegateRedisClusterCommandAndLift(_.hrandfieldWithvalues(key, count)).map(_.asScala.collect {
      case kv if kv.hasValue => kv.getKey -> kv.getValue
    }.toMap)

  override def hScan(
    key: K,
    cursor: RedisKeyAsyncCommands.ScanCursor = RedisKeyAsyncCommands.ScanCursor.Initial,
    limit: Option[Long] = None,
    matchPattern: Option[String] = None
  ): Future[(RedisKeyAsyncCommands.ScanCursor, Map[K, V])] = {
    val scanArgs = (limit, matchPattern) match {
      case (Some(limitValue), Some(matchPatternValue)) =>
        Some(ScanArgs.Builder.limit(limitValue).`match`(matchPatternValue))
      case (Some(limitValue), None) =>
        Some(ScanArgs.Builder.limit(limitValue))
      case (None, Some(matchPatternValue)) =>
        Some(ScanArgs.Builder.matches(matchPatternValue))
      case _ =>
        None
    }
    val lettuceCursor = io.lettuce.core.ScanCursor.of(cursor.cursor)
    lettuceCursor.setFinished(cursor.finished)
    val response = scanArgs match {
      case Some(scanArgs) =>
        delegateRedisClusterCommandAndLift(_.hscan(key, lettuceCursor, scanArgs))
      case None =>
        delegateRedisClusterCommandAndLift(_.hscan(key, lettuceCursor))
    }
    response.map{ result =>
      (
        RedisKeyAsyncCommands.ScanCursor(result.getCursor, finished = result.isFinished),
        result.getMap.asScala.toMap
      )
    }
  }


  override def hKeys(key: K): Future[List[K]] =
    delegateRedisClusterCommandAndLift(_.hkeys(key)).map(_.asScala.toList)

  override def hLen(key: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.hlen(key)).map(Long2long)

  override def hMGet(key: K, fields: K*): Future[ListMap[K, Option[V]]] =
    delegateRedisClusterCommandAndLift(_.hmget(key, fields: _*)).map(_.asScala.map{ kv =>
      kv.getKey -> (if(kv.hasValue) Some(kv.getValue) else None)
    }).map(ListMap.from)

  override def hSet(key: K, field: K, value: V): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.hset(key, field, value)).map(Boolean2boolean)

  override def hMSet(key: K, values: Map[K, V]): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.hmset(key, values.asJava)).map(_ => ())

  override def hSetNx(key: K, field: K, value: V): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.hsetnx(key, field, value)).map(Boolean2boolean)

  override def hStrLen(key: K, field: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.hstrlen(key, field)).map(Long2long)

  override def hVals(key: K): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.hvals(key)).map(_.asScala.toList)

}
package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.jdk.CollectionConverters.CollectionHasAsScala

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands.{InitialCursor, ScanResults}
import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation



private[arugula] trait LettuceRedisSetAsyncCommands[K, V] extends RedisSetAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {

  override def sAdd(key: K, values: V*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.sadd(key, values: _*)).map(Long2long)

  override def sCard(key: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.scard(key)).map(Long2long)

  override def sDiff(keys: K*): Future[Set[V]] =
    delegateRedisClusterCommandAndLift(_.sdiff(keys: _*)).map(_.asScala.toSet)

  override def sDiffStore(destination: K, keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.sdiffstore(destination, keys: _*)).map(Long2long)

  override def sInter(keys: K*): Future[Set[V]] =
    delegateRedisClusterCommandAndLift(_.sinter(keys: _*)).map(_.asScala.toSet)

  override def sInterCard(keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.sintercard(keys: _*)).map(Long2long)

  override def sInterStore(destination: K, keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.sinterstore(destination, keys: _*)).map(Long2long)

  override def sIsMember(key: K, value: V): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.sismember(key, value)).map(Boolean2boolean)

  override def sMembers(key: K): Future[Set[V]] =
    delegateRedisClusterCommandAndLift(_.smembers(key)).map(_.asScala.toSet)

  override def smIsMember(key: K, values: V*): Future[List[Boolean]] =
    delegateRedisClusterCommandAndLift(_.smismember(key, values:_*)).map(_.asScala.toList.map(Boolean2boolean))

  override def sMove(source: K, destination: K, member: V): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.smove(source, destination, member)).map(Boolean2boolean)

  override def sPop(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.spop(key)).map(Option.apply)

  override def sRandMember(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.srandmember(key)).map(Option.apply)

  override def sRandMember(key: K, count: Long): Future[Set[V]] =
    delegateRedisClusterCommandAndLift(_.srandmember(key, count)).map(_.asScala.toSet)

  override def sRem(key: K, values: V*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.srem(key, values: _*)).map(Long2long)

  override def sUnion(keys: K*): Future[Set[V]] =
    delegateRedisClusterCommandAndLift(_.sunion(keys: _*)).map(_.asScala.toSet)

  override def sUnionStore(destination: K, keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.sunionstore(destination, keys: _*)).map(Long2long)

  override def sScan(
    key: K,
    cursor: String = InitialCursor,
    limit: Option[Long] = None,
    matchPattern: Option[String] = None
  ): Future[ScanResults[Set[V]]] = {
    val scanArgs = (limit, matchPattern) match {
      case (Some(limitValue), Some(matchPatternValue)) =>
        Some(io.lettuce.core.ScanArgs.Builder.limit(limitValue).`match`(matchPatternValue))
      case (Some(limitValue), None) =>
        Some(io.lettuce.core.ScanArgs.Builder.limit(limitValue))
      case (None, Some(matchPatternValue)) =>
        Some(io.lettuce.core.ScanArgs.Builder.matches(matchPatternValue))
      case _ =>
        None
    }
    val lettuceCursor = io.lettuce.core.ScanCursor.of(cursor)
    val response = scanArgs match {
      case Some(scanArgs) =>
        delegateRedisClusterCommandAndLift(_.sscan(key, lettuceCursor, scanArgs))
      case None =>
        delegateRedisClusterCommandAndLift(_.sscan(key, lettuceCursor))
    }
    response.map{ result =>
      ScanResults(
        cursor = result.getCursor,
        finished = result.isFinished,
        result.getValues.asScala.toSet
      )
    }
  }

}
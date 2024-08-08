package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation
import io.lettuce.core.{LMPopArgs, LMoveArgs}

private[arugula] trait LettuceRedisListAsyncCommands[K, V] extends RedisListAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V]{

  override def blMove(
    source: K,
    destination: K,
    sourceSide: RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Right,
    destinationSide: RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Left,
    timeout: Double = 0.0, // zero is infinite wait
  ): Future[Option[V]] = {
    val args = (sourceSide, destinationSide) match {
      case (RedisListAsyncCommands.Side.Left, RedisListAsyncCommands.Side.Left) => LMoveArgs.Builder.leftLeft()
      case (RedisListAsyncCommands.Side.Left, RedisListAsyncCommands.Side.Right) => LMoveArgs.Builder.leftRight()
      case (RedisListAsyncCommands.Side.Right, RedisListAsyncCommands.Side.Left) => LMoveArgs.Builder.rightLeft()
      case (RedisListAsyncCommands.Side.Right, RedisListAsyncCommands.Side.Right) => LMoveArgs.Builder.rightRight()
    }
    delegateRedisClusterCommandAndLift(_.blmove(source, destination, args, timeout)).map(Option.apply)
  }

  override def blMPop(
    keys: List[K],
    direction:RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Left,
    count: Int = 1,
    timeout: Double = 0.0,
  ): Future[Option[(K, List[V])]] = {
    val args: LMPopArgs = direction match {
      case RedisListAsyncCommands.Side.Left => LMPopArgs.Builder.left().count(count)
      case RedisListAsyncCommands.Side.Right => LMPopArgs.Builder.right().count(count)
    }
    delegateRedisClusterCommandAndLift(_.blmpop(timeout, args, keys: _*)).map{
      case null => None
      case result =>
        val key = result.getKey
        val values = if(result.hasValue) result.getValue.asScala.toList else List.empty
        Some((key, values))
    }
  }

  def lMPop(
    keys: List[K],
    direction:RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Left,
    count: Int = 1,
  ): Future[Option[(K, List[V])]] = {
    val args: LMPopArgs = direction match {
      case RedisListAsyncCommands.Side.Left => LMPopArgs.Builder.left().count(count)
      case RedisListAsyncCommands.Side.Right => LMPopArgs.Builder.right().count(count)
    }
    delegateRedisClusterCommandAndLift(_.lmpop(args, keys: _*)).map{
      case null => None
      case result =>
        val key = result.getKey
        val values = if(result.hasValue) result.getValue.asScala.toList else List.empty
        Some((key, values))
    }
  }

  override def lRem(key: K, count: Long, value: V): Future[Long] =
    delegateRedisClusterCommandAndLift(_.lrem(key, count, value)).map(Long2long)

  override def lTrim(key: K, start: Long, stop: Long): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.ltrim(key, start, stop)).map(_ => ())

  override def lRange(key: K, start: Long, stop: Long): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.lrange(key, start, stop)).map(_.asScala.toList)

  def lMove(
    source: K,
    destination: K,
    sourceSide: RedisListAsyncCommands.Side,
    destinationSide: RedisListAsyncCommands.Side
  ): Future[Option[V]] = {
    val args = (sourceSide, destinationSide) match {
      case (RedisListAsyncCommands.Side.Left, RedisListAsyncCommands.Side.Left) => LMoveArgs.Builder.leftLeft()
      case (RedisListAsyncCommands.Side.Left, RedisListAsyncCommands.Side.Right) => LMoveArgs.Builder.leftRight()
      case (RedisListAsyncCommands.Side.Right, RedisListAsyncCommands.Side.Left) => LMoveArgs.Builder.rightLeft()
      case (RedisListAsyncCommands.Side.Right, RedisListAsyncCommands.Side.Right) => LMoveArgs.Builder.rightRight()
    }
    delegateRedisClusterCommandAndLift(_.lmove(source, destination, args)).map(Option.apply)
  }

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

object LettuceRedisListAsyncCommands {
  sealed trait Side

  object Side {
    case object Left extends Side

    case object Right extends Side
  }

}

/**
 * blpop
 * brpop
 * brpoplpush
 * lindex
 * linsert
 * llen
 * lpop
 * lpos
 * lpush
 * lpushx
 * lrange
 * lrem
 * lset
 * ltrim
 * rpop
 * rpoplpush
 * rpush
 * rpushx
 */
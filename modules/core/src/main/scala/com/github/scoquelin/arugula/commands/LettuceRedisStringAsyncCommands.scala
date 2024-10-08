package com.github.scoquelin.arugula.commands

import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.commands.RedisStringAsyncCommands.{BitFieldCommand, BitFieldDataType, BitFieldOperation}
import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation
import io.lettuce.core.{BitFieldArgs, GetExArgs, KeyValue}

import java.util.concurrent.TimeUnit

private[arugula] trait LettuceRedisStringAsyncCommands[K, V] extends RedisStringAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {

  override def append(key: K, value: V): Future[Long] =
    delegateRedisClusterCommandAndLift(_.append(key, value)).map(Long2long)

  override def bitCount(key: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.bitcount(key)).map(Long2long)

  override def bitCount(key: K, start: Long, end: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.bitcount(key, start, end)).map(Long2long)

  override def bitField(key: K, commands: Seq[BitFieldCommand]): Future[Seq[Long]] = {
    val args = new BitFieldArgs()

    commands.foreach{ command =>
      val bitFieldType = command.dataType match {
        case BitFieldDataType.Signed(width) => BitFieldArgs.signed(width)
        case BitFieldDataType.Unsigned(width) => BitFieldArgs.unsigned(width)
      }
      command match {
        case BitFieldCommand(BitFieldOperation.Set, _, Some(offset), value) =>
          args.set(bitFieldType, offset, value.getOrElse(0L))

        case BitFieldCommand(BitFieldOperation.Set, _, _, value) =>
          args.set(bitFieldType, value.getOrElse(0L))

        case BitFieldCommand(BitFieldOperation.Get, _, Some(offset), _) =>
          args.get(bitFieldType, offset)

        case BitFieldCommand(BitFieldOperation.Get, _, _, _) =>
          args.get(bitFieldType)

        case BitFieldCommand(BitFieldOperation.Incrby, _, Some(offset), Some(value)) =>
          args.incrBy(bitFieldType, offset, value)

        case BitFieldCommand(BitFieldOperation.Incrby, _, _, Some(value)) =>
          args.incrBy(bitFieldType, value)

        case BitFieldCommand(BitFieldOperation.OverflowSat, _, _, _) =>
          args.overflow(BitFieldArgs.OverflowType.SAT)

        case BitFieldCommand(BitFieldOperation.OverflowFail, _, _, _) =>
          args.overflow(BitFieldArgs.OverflowType.FAIL)

        case BitFieldCommand(BitFieldOperation.OverflowWrap, _, _, _) =>
          args.overflow(BitFieldArgs.OverflowType.WRAP)

        case _ =>
          throw new IllegalArgumentException("Invalid BitFieldCommand parameters")
      }
    }

    delegateRedisClusterCommandAndLift(_.bitfield(key, args)).map(_.asScala.toSeq.map(_.longValue()))
  }

  override def bitOpAnd(destination: K, keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.bitopAnd(destination, keys: _*)).map(Long2long)

  override def bitOpOr(destination: K, keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.bitopOr(destination, keys: _*)).map(Long2long)

  override def bitOpXor(destination: K, keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.bitopXor(destination, keys: _*)).map(Long2long)

  override def bitOpNot(destination: K, source: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.bitopNot(destination, source)).map(Long2long)

  override def bitPos(key: K, state: Boolean): Future[Long] =
    delegateRedisClusterCommandAndLift(_.bitpos(key, state)).map(Long2long)

  override def bitPos(key: K, state: Boolean, start: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.bitpos(key, state, start)).map(Long2long)

  override def bitPos(key: K, state: Boolean, start: Long, end: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.bitpos(key, state, start, end)).map(Long2long)

  override def getBit(key: K, offset: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.getbit(key, offset)).map(Long2long)

  override def get(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.get(key)).map(Option.apply)

  override def getDel(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.getdel(key)).map(Option.apply)

  override def getEx(key: K, expiresIn: FiniteDuration): Future[Option[V]] =
    (expiresIn.unit match {
      case TimeUnit.MILLISECONDS | TimeUnit.MICROSECONDS | TimeUnit.NANOSECONDS =>
        delegateRedisClusterCommandAndLift(_.getex(key, GetExArgs.Builder.ex(expiresIn.toMillis)))
      case _ =>
        delegateRedisClusterCommandAndLift(_.getex(key, GetExArgs.Builder.ex(java.time.Duration.ofSeconds(expiresIn.toSeconds))))
    }).map(Option.apply)

  override def getRange(key: K, start: Long, end: Long): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.getrange(key, start, end)).map(Option.apply)

  override def getSet(key: K, value: V): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.getset(key, value)).map(Option.apply)

  override def mGet(keys: K*): Future[ListMap[K, Option[V]]] =
    delegateRedisClusterCommandAndLift(_.mget(keys: _*)).map {
      case null => ListMap.empty
      case kvs => ListMap.from(kvs.asScala.collect {
        case keyValue =>
          if (keyValue.hasValue) keyValue.getKey -> Some(keyValue.getValue) else keyValue.getKey -> None
      })
    }

  override def mSet(keyValues: Map[K, V]): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.mset(keyValues.asJava)).map(_ => ())

  override def mSetNx(keyValues: Map[K, V]): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.msetnx(keyValues.asJava)).map(Boolean2boolean)

  override def set(key: K, value: V): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.set(key, value)).map(_ => ())

  override def setBit(key: K, offset: Long, value: Int): Future[Long] =
    delegateRedisClusterCommandAndLift(_.setbit(key, offset, value)).map(Long2long)

  override def setEx(key: K, value: V, expiresIn: FiniteDuration): Future[Unit] =
    (expiresIn.unit match {
      case TimeUnit.MILLISECONDS | TimeUnit.MICROSECONDS | TimeUnit.NANOSECONDS =>
        delegateRedisClusterCommandAndLift(_.psetex(key, expiresIn.toMillis, value))
      case _ =>
        delegateRedisClusterCommandAndLift(_.setex(key, expiresIn.toSeconds, value))
    }).map(_ => ())

  override def setNx(key: K, value: V): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.setnx(key, value)).map(Boolean2boolean)

  override def setRange(key: K, offset: Long, value: V): Future[Long] =
    delegateRedisClusterCommandAndLift(_.setrange(key, offset, value)).map(Long2long)

  override def strLen(key: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.strlen(key)).map(Long2long)

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

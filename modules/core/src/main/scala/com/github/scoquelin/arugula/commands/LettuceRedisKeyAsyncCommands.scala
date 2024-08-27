package com.github.scoquelin.arugula.commands

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands.InitialCursor
import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation
import io.lettuce.core.{CopyArgs, ScanCursor}

import java.time.Instant
import java.util.concurrent.TimeUnit

private[arugula] trait LettuceRedisKeyAsyncCommands[K, V] extends RedisKeyAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {
  import LettuceRedisKeyAsyncCommands.toFiniteDuration

  override def copy(srcKey: K, destKey: K): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.copy(srcKey, destKey)).map(Boolean2boolean)

  override def copy(srcKey: K, destKey: K, args: RedisKeyAsyncCommands.CopyArgs): Future[Unit] = {
    val copyArgs: CopyArgs = CopyArgs.Builder.replace(args.replace)
    args.destinationDb.foreach(copyArgs.destinationDb(_))
    delegateRedisClusterCommandAndLift(_.copy(srcKey, destKey, copyArgs)).map(_ => ())
  }

  override def del(key: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.del(key: _*)).map(Long2long)

  override def unlink(key: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.unlink(key: _*)).map(Long2long)

  override def dump(key: K): Future[Array[Byte]] =
    delegateRedisClusterCommandAndLift(_.dump(key))

  override def exists(key: K*): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.exists(key: _*)).map(_ == key.size.toLong)

  override def expire(key: K, expiresIn: FiniteDuration): Future[Boolean] =
    (expiresIn.unit match {
      case TimeUnit.MILLISECONDS | TimeUnit.MICROSECONDS | TimeUnit.NANOSECONDS =>
        delegateRedisClusterCommandAndLift(_.pexpire(key, expiresIn.toMillis))
      case _ =>
        delegateRedisClusterCommandAndLift(_.expire(key, expiresIn.toSeconds))
    }).map(Boolean2boolean)


  override def expireAt(key: K, timestamp: Instant): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.pexpireat(key, timestamp.toEpochMilli)).map(Boolean2boolean)

  override def expireTime(key: K): Future[Option[Instant]] = {
    delegateRedisClusterCommandAndLift(_.pexpiretime(key)).map {
      case d if d < 0 => None
      case d => Some(Instant.ofEpochMilli(d))
    }
  }

  override def keys(pattern: K): Future[List[K]] =
    delegateRedisClusterCommandAndLift(_.keys(pattern)).map(_.toList)

  override def move(key: K, db: Int): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.move(key, db)).map(Boolean2boolean)

  override def rename(key: K, newKey: K): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.rename(key, newKey)).map(_ => ())

  override def renameNx(key: K, newKey: K): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.renamenx(key, newKey)).map(Boolean2boolean)

  override def restore(key: K, serializedValue: Array[Byte], args: RedisKeyAsyncCommands.RestoreArgs = RedisKeyAsyncCommands.RestoreArgs()): Future[Unit] = {
    val restoreArgs = new io.lettuce.core.RestoreArgs()
    args.ttl.foreach { duration =>
      restoreArgs.ttl(duration.toMillis)
    }
    args.idleTime.foreach { duration =>
      restoreArgs.idleTime(duration.toMillis)
    }
    args.frequency.foreach { frequency =>
      restoreArgs.frequency(frequency)
    }
    if(args.replace) restoreArgs.replace()
    args.absTtl.foreach{ instant =>
      restoreArgs.absttl(true)
      restoreArgs.ttl(instant.toEpochMilli)
    }
    delegateRedisClusterCommandAndLift(_.restore(key, serializedValue, restoreArgs)).map(_ => ())
  }

  override def scan(cursor: String = InitialCursor, matchPattern: Option[String] = None, limit: Option[Int] = None): Future[RedisBaseAsyncCommands.ScanResults[List[K]]] = {
    val scanArgs = (matchPattern, limit) match {
      case (Some(pattern), Some(count)) => Some(io.lettuce.core.ScanArgs.Builder.matches(pattern).limit(count))
      case (Some(pattern), None) => Some(io.lettuce.core.ScanArgs.Builder.matches(pattern))
      case (None, Some(count)) => Some(io.lettuce.core.ScanArgs.Builder.limit(count))
      case _ => None
    }
    val result = scanArgs match {
      case Some(args) => delegateRedisClusterCommandAndLift(_.scan(ScanCursor.of(cursor), args))
      case None => delegateRedisClusterCommandAndLift(_.scan(ScanCursor.of(cursor)))
    }
    result.map { scanResult =>
      RedisBaseAsyncCommands.ScanResults(scanResult.getCursor, scanResult.isFinished, scanResult.getKeys.toList)
    }
  }

  override def ttl(key: K): Future[Option[FiniteDuration]] =
    delegateRedisClusterCommandAndLift(_.pttl(key)).map(toFiniteDuration(TimeUnit.MILLISECONDS))

  override def touch(key: K*): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.touch(key: _*)).map(Long2long)
  }

  override def `type`(key: K): Future[String] = {
    delegateRedisClusterCommandAndLift(_.`type`(key))
  }
}

private[this] object LettuceRedisKeyAsyncCommands {
  private[commands] def toFiniteDuration(units: TimeUnit)(duration: java.lang.Long): Option[FiniteDuration] =
    duration match {
      case d if d < 0 => None
      case d => Some(FiniteDuration(d, units))
    }
}
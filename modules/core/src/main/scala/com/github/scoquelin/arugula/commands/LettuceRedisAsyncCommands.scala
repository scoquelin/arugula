package com.github.scoquelin.arugula.commands

import com.github.scoquelin.arugula.api.commands.RedisKeyAsyncCommands.ScanCursor
import com.github.scoquelin.arugula.api.commands.RedisSortedSetAsyncCommands.ZAddOptions._
import com.github.scoquelin.arugula.api.commands.RedisSortedSetAsyncCommands._
import com.github.scoquelin.arugula.api.commands.RedisAsyncCommands
import com.github.scoquelin.arugula.connection.RedisConnection

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import io.lettuce.core.cluster.api.async.{RedisClusterAsyncCommands => JRedisClusterAsyncCommands}
import io.lettuce.core.{Limit, Range, RedisFuture, ScanArgs, ScoredValue, ZAddArgs}

import java.util.concurrent.TimeUnit

/**
 * This is the Lettuce implementation of the RedisAsyncCommands API.
 * This implementation is meant to evolve and any Redis operation that is supported by the Lettuce API could be potentially added here.
 *
 * @param connection The underlying Lettuce connection that will be used to relay commands.
 * @param cluster Flag explicitly conveying if we are if a cluster-enabled environment(client/server) or not
 * @param ec The execution context
 * @tparam K type of the key
 * @tparam V type of the value
 */
private[commands] class LettuceRedisAsyncCommands[K, V](connection: RedisConnection[K, V], cluster: Boolean)(implicit ec: ExecutionContext) extends RedisAsyncCommands[K, V] {

  //Generalizing over JRedisClusterAsyncCommands for both single-node and cluster commands as we don't need transaction commands (that are single-node only)
  //(relying on the fact that io.lettuce.core.api.async.RedisAsyncCommands also extends io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands)
  val lettuceRedisClusterAsyncCommands: Future[JRedisClusterAsyncCommands[K, V]] =
    if (cluster) connection.clusterAsync else connection.async

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

  override def get(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.get(key)).map(Option.apply)

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

  override def hSetNx(key: K, field: K, value: V): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.hsetnx(key, field, value)).map(Boolean2boolean)

  override def flushAll: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.flushall()).map(_ => ())

  override def info: Future[Map[String, String]] = {
    def parseInfo(info: String): Map[String, String] =
      info
        .split("\\r?\\n")
        .toList
        .map(_.split(":", 2).toList)
        .collect { case k :: v :: Nil => (k, v) }
        .toMap

    delegateRedisClusterCommandAndLift(_.info()).map(parseInfo)
  }

  override def ping: Future[String] =
    delegateRedisClusterCommandAndLift(_.ping())

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

  override def ttl(key: K): Future[Option[FiniteDuration]] =
    delegateRedisClusterCommandAndLift(_.ttl(key)).map(toFiniteDuration(TimeUnit.SECONDS))

  override def pipeline(commands: RedisAsyncCommands[K, V] => List[Future[Any]]): Future[Option[List[Any]]] = {
    //Resorting to basic Futures sequencing after issues trying to implement https://github.com/lettuce-io/lettuce-core/wiki/Pipelining-and-command-flushing#command-flushing
    commands(this).foldLeft(Future.successful(Option.empty[List[Any]]))((commandsOutcome, nextFuture) => commandsOutcome.flatMap {
      case Some(existingList) =>
        nextFuture.map(nextFutureOutcome => Some(existingList ::: List(nextFutureOutcome)))
      case None =>
        nextFuture.map(nextFutureOutcome => Some(List(nextFutureOutcome)))
    })
  }

  override def zAdd(key: K, args: Option[ZAddOptions], values: ScoreWithValue[V]*): Future[Long] = {
    ((args match {
      case Some(zAddOption) => zAddOption match {
        case NX => Some(ZAddArgs.Builder.nx())
        case XX => Some(ZAddArgs.Builder.xx())
        case LT => Some(ZAddArgs.Builder.lt())
        case GT => Some(ZAddArgs.Builder.gt())
        case CH => Some(ZAddArgs.Builder.ch())
      }
      case _ => None
    }) match {
      case Some(zAddArgs) =>
        delegateRedisClusterCommandAndLift(_.zadd(key, zAddArgs, values.map(scoreWithValue => ScoredValue.just(scoreWithValue.score, scoreWithValue.value)): _*))
      case None =>
        delegateRedisClusterCommandAndLift(_.zadd(key, values.map(scoreWithValue => ScoredValue.just(scoreWithValue.score, scoreWithValue.value)): _*))
    }).map(Long2long)
  }

  override def zPopMin(key: K, count: Long): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zpopmin(key, count)).map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zPopMax(key: K, count: Long): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zpopmax(key, count)).map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zRangeByScore[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit] = None): Future[List[V]] =
    limit match {
      case Some(rangeLimit) =>
        delegateRedisClusterCommandAndLift(_.zrangebyscore(key, toJavaNumberRange(range), Limit.create(rangeLimit.offset, rangeLimit.count))).map(_.asScala.toList)
      case None =>
        delegateRedisClusterCommandAndLift(_.zrangebyscore(key, toJavaNumberRange(range))).map(_.asScala.toList)
    }

  override def zRevRangeByScore[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit] = None): Future[List[V]] =
    limit match {
      case Some(rangeLimit) =>
        delegateRedisClusterCommandAndLift(_.zrevrangebyscore(key, toJavaNumberRange(range), Limit.create(rangeLimit.offset, rangeLimit.count))).map(_.asScala.toList)
      case None =>
        delegateRedisClusterCommandAndLift(_.zrevrangebyscore(key, toJavaNumberRange(range))).map(_.asScala.toList)
    }

  override def zRangeWithScores(key: K, start: Long, stop: Long): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zrangeWithScores(key, start, stop))
      .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zScan(key: K, cursor: ScanCursor = ScanCursor.Initial, limit: Option[Long] = None, matchPattern: Option[String] = None): Future[ScanCursorWithScoredValues[V]] = {
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

    scanArgs match {
      case Some(args) =>
        delegateRedisClusterCommandAndLift(_.zscan(key, io.lettuce.core.ScanCursor.of(cursor.cursor), args)).map { scanResult =>
          ScanCursorWithScoredValues(ScanCursor(scanResult.getCursor, scanResult.isFinished), scanResult.getValues.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))
        }
      case None =>
        delegateRedisClusterCommandAndLift(_.zscan(key, io.lettuce.core.ScanCursor.of(cursor.cursor))).map { scanResult =>
          ScanCursorWithScoredValues(ScanCursor(scanResult.getCursor, scanResult.isFinished), scanResult.getValues.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))
        }
    }
  }

  override def zRem(key: K, values: V*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zrem(key, values: _*)).map(Long2long)

  override def zRemRangeByRank(key: K, start: Long, stop: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zremrangebyrank(key, start, stop)).map(Long2long)

  override def zRemRangeByScore[T: Numeric](key: K, range: ZRange[T]): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zremrangebyscore(key, toJavaNumberRange(range))).map(Long2long)

  /**
   * Lifts the outcome (RedisFuture[T]) of a Lettuce Redis command into a Scala Future[T]
   *
   * @param command The Lettuce command to be dispatched
   * @tparam T The type of the returned value inside the io.lettuce.core.RedisFuture[T]
   * @return A Scala Future[T] extracted from the io.lettuce.core.RedisFuture[T]
   */
  private def delegateRedisClusterCommandAndLift[T](command: JRedisClusterAsyncCommands[K, V] => RedisFuture[T]): Future[T] =
    lettuceRedisClusterAsyncCommands.flatMap(redisCommands => command(redisCommands).toCompletableFuture.asScala)

  private def toFiniteDuration(units: TimeUnit)(duration: java.lang.Long): Option[FiniteDuration] =
    duration match {
      case d if d < 0 => None
      case d => Some(FiniteDuration(d, units))
    }

  private def toJavaNumberRange[T: Numeric](range: ZRange[T]): Range[Number] = {
    def toJavaNumber(t: T): java.lang.Number = t match {
      case b: Byte => b
      case s: Short => s
      case i: Int => i
      case l: Long => l
      case f: Float => f
      case _ => implicitly[Numeric[T]].toDouble(t)
    }
    Range.create(toJavaNumber(range.start), toJavaNumber(range.end))
  }
}

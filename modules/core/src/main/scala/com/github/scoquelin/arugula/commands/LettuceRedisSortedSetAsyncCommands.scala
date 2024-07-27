package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

import com.github.scoquelin.arugula.commands.RedisKeyAsyncCommands.ScanCursor
import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands.ZAddOptions.{CH, GT, LT, NX, XX}
import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands.{RangeLimit, ScanCursorWithScoredValues, ScoreWithValue, ZAddOptions, ZRange}
import io.lettuce.core.{Limit, Range, ScanArgs, ScoredValue, ZAddArgs}
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

private[arugula] trait LettuceRedisSortedSetAsyncCommands[K, V] extends RedisSortedSetAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {
  import LettuceRedisSortedSetAsyncCommands.toJavaNumberRange
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

}

private[this] object LettuceRedisSortedSetAsyncCommands{
  private[commands] def toJavaNumberRange[T: Numeric](range: ZRange[T]): Range[Number] = {
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
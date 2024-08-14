package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands.{SortOrder, RangeLimit, ScoreWithKeyValue, ScoreWithValue, ZAddOptions, ZRange}
import io.lettuce.core.{Limit, Range, ScanArgs, ScoredValue, ZAddArgs}
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands.{InitialCursor, ScanResults}
import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

private[arugula] trait LettuceRedisSortedSetAsyncCommands[K, V] extends RedisSortedSetAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {

  import LettuceRedisSortedSetAsyncCommands.toJavaNumberRange

  override def bzMPop(timeout: FiniteDuration, direction: SortOrder, keys: K*): Future[Option[ScoreWithKeyValue[K, V]]] = {
    val args = direction match {
      case SortOrder.Min => io.lettuce.core.ZPopArgs.Builder.min()
      case SortOrder.Max => io.lettuce.core.ZPopArgs.Builder.max()
    }
    delegateRedisClusterCommandAndLift(_.bzmpop(timeout.toMillis.toDouble / 1000, args, keys: _*)).map {
      case null => None
      case scoredValue if scoredValue.hasValue =>
        if(scoredValue.getValue.hasValue){
          Some(ScoreWithKeyValue(scoredValue.getValue.getScore, scoredValue.getKey, scoredValue.getValue.getValue))
        } else {
          None
        }
      case _ => None
    }
  }

  override def bzMPop(timeout: FiniteDuration,
    count: Int,
    direction: SortOrder,
    keys: K*): Future[List[ScoreWithKeyValue[K, V]]] = {
    val args = direction match {
      case SortOrder.Min => io.lettuce.core.ZPopArgs.Builder.min()
      case SortOrder.Max => io.lettuce.core.ZPopArgs.Builder.max()
    }
    delegateRedisClusterCommandAndLift(_.bzmpop(timeout.toMillis.toDouble / 1000, count, args, keys: _*)).map{ result =>
      if(result.hasValue){
        val key = result.getKey
        result.getValue.asScala.toList.collect{
          case scoredValue if scoredValue.hasValue => ScoreWithKeyValue(scoredValue.getScore, key, scoredValue.getValue)
        }
      } else {
        List.empty
      }
    }
  }

  override def bzPopMin(timeout: FiniteDuration, keys: K*): Future[Option[ScoreWithKeyValue[K, V]]] =
    delegateRedisClusterCommandAndLift(_.bzpopmin(timeout.toMillis.toDouble / 1000, keys: _*)).map {
      case null => None
      case scoredValue if scoredValue.hasValue =>
        if(scoredValue.getValue.hasValue){
          Some(ScoreWithKeyValue(scoredValue.getValue.getScore, scoredValue.getKey, scoredValue.getValue.getValue))
        } else {
          None
        }
      case _ => None
    }

  override def bzPopMax(timeout: FiniteDuration, keys: K*): Future[Option[ScoreWithKeyValue[K, V]]] =
    delegateRedisClusterCommandAndLift(_.bzpopmax(timeout.toMillis.toDouble/1000, keys: _*)).map {
      case null => None
      case scoredValue if scoredValue.hasValue =>
        if(scoredValue.getValue.hasValue){
          Some(ScoreWithKeyValue(scoredValue.getValue.getScore, scoredValue.getKey, scoredValue.getValue.getValue))
        } else {
          None
        }
      case _ => None
    }

  override def zAdd(key: K, values: ScoreWithValue[V]*): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.zadd(key, values.map(scoreWithValue => ScoredValue.just(scoreWithValue.score, scoreWithValue.value)): _*)).map(Long2long)
  }

  override def zAdd(key: K, args: Set[ZAddOptions], values: ScoreWithValue[V]*): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.zadd(key, LettuceRedisSortedSetAsyncCommands.zAddOptionsToJava(args), values.map(scoreWithValue => ScoredValue.just(scoreWithValue.score, scoreWithValue.value)): _*)).map(Long2long)
  }

  override def zAddIncr(key: K, score: Double, member: V): Future[Option[Double]] =
    delegateRedisClusterCommandAndLift(_.zaddincr(key, score, member)).map(Option(_).map(Double2double))

  override def zAddIncr(key: K, args: Set[ZAddOptions], score: Double, member: V): Future[Option[Double]] = {
    delegateRedisClusterCommandAndLift(_.zaddincr(key, LettuceRedisSortedSetAsyncCommands.zAddOptionsToJava(args), score, member)).map(Option(_).map(Double2double))
  }

  override def zCard(key: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zcard(key)).map(Long2long)

  override def zCount[T: Numeric](key: K, range: ZRange[T]): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zcount(key, toJavaNumberRange(range))).map(Long2long)

  override def zMPop(direction: SortOrder, keys: K*): Future[Option[ScoreWithKeyValue[K, V]]] = {
    val args = direction match {
      case SortOrder.Min => io.lettuce.core.ZPopArgs.Builder.min()
      case SortOrder.Max => io.lettuce.core.ZPopArgs.Builder.max()
    }
    delegateRedisClusterCommandAndLift(_.zmpop(args, keys: _*)).map {
      case null => None
      case scoredValue if scoredValue.hasValue =>
        if(scoredValue.getValue.hasValue){
          Some(ScoreWithKeyValue(scoredValue.getValue.getScore, scoredValue.getKey, scoredValue.getValue.getValue))
        } else {
          None
        }
      case _ => None
    }
  }

  override def zMPop(count: Int, direction: SortOrder, keys: K*): Future[List[ScoreWithKeyValue[K, V]]] = {
    val args = direction match {
      case SortOrder.Min => io.lettuce.core.ZPopArgs.Builder.min()
      case SortOrder.Max => io.lettuce.core.ZPopArgs.Builder.max()
    }
    delegateRedisClusterCommandAndLift(_.zmpop(count, args, keys: _*)).map{ result =>
      if(result.hasValue){
        val key = result.getKey
        result.getValue.asScala.toList.collect{
          case scoredValue if scoredValue.hasValue => ScoreWithKeyValue(scoredValue.getScore, key, scoredValue.getValue)
        }
      } else {
        List.empty
      }
    }
  }

  override def zPopMin(key: K): Future[Option[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zpopmin(key)).map {
      case null => None
      case scoredValue if scoredValue.hasValue => Some(ScoreWithValue(scoredValue.getScore, scoredValue.getValue))
      case _ => None
    }

  override def zPopMin(key: K, count: Long): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zpopmin(key, count)).map(_.asScala.toList.collect {
      case scoredValue if scoredValue.hasValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)
    })

  override def zPopMax(key: K): Future[Option[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zpopmax(key)).map {
      case null => None
      case scoredValue if scoredValue.hasValue => Some(ScoreWithValue(scoredValue.getScore, scoredValue.getValue))
      case _ => None
    }

  override def zPopMax(key: K, count: Long): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zpopmax(key, count)).map(_.asScala.toList.collect {
      case scoredValue if scoredValue.hasValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)
    })

  override def zScore(key: K, value: V): Future[Option[Double]] =
    delegateRedisClusterCommandAndLift(_.zscore(key, value)).map(Option(_).map(Double2double))

  override def zRange(key: K, start: Long, stop: Long): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.zrange(key, start, stop)).map(_.asScala.toList)

  override def zRangeByScore[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit] = None): Future[List[V]] =
    limit match {
      case Some(rangeLimit) =>
        delegateRedisClusterCommandAndLift(_.zrangebyscore(key, toJavaNumberRange(range), Limit.create(rangeLimit.offset, rangeLimit.count))).map(_.asScala.toList)
      case None =>
        delegateRedisClusterCommandAndLift(_.zrangebyscore(key, toJavaNumberRange(range))).map(_.asScala.toList)
    }

  override def zRangeByScoreWithScores[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit] = None): Future[List[ScoreWithValue[V]]] =
    limit match {
      case Some(rangeLimit) =>
        delegateRedisClusterCommandAndLift(_.zrangebyscoreWithScores(key, toJavaNumberRange(range), Limit.create(rangeLimit.offset, rangeLimit.count)))
          .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))
      case None =>
        delegateRedisClusterCommandAndLift(_.zrangebyscoreWithScores(key, toJavaNumberRange(range)))
          .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))
    }

  override def zRevRangeByScore[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit] = None): Future[List[V]] =
    limit match {
      case Some(rangeLimit) =>
        delegateRedisClusterCommandAndLift(_.zrevrangebyscore(key, toJavaNumberRange(range), Limit.create(rangeLimit.offset, rangeLimit.count))).map(_.asScala.toList)
      case None =>
        delegateRedisClusterCommandAndLift(_.zrevrangebyscore(key, toJavaNumberRange(range))).map(_.asScala.toList)
    }

  override def zRevRangeByScoreWithScores[T: Numeric](
    key: K,
    range: ZRange[T],
    limit: Option[RangeLimit] = None
  ): Future[List[ScoreWithValue[V]]] =
    limit match {
      case Some(rangeLimit) =>
        delegateRedisClusterCommandAndLift(_.zrevrangebyscoreWithScores(key, toJavaNumberRange(range), Limit.create(rangeLimit.offset, rangeLimit.count)))
          .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))
      case None =>
        delegateRedisClusterCommandAndLift(_.zrevrangebyscoreWithScores(key, toJavaNumberRange(range)))
          .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))
    }

  override def zIncrBy(key: K, amount: Double, value: V): Future[Double] =
    delegateRedisClusterCommandAndLift(_.zincrby(key, amount, value)).map(Double2double)

  override def zRank(key: K, value: V): Future[Option[Long]] =
    delegateRedisClusterCommandAndLift(_.zrank(key, value)).map(Option(_).map(Long2long))

  override def zRankWithScore(key: K, value: V): Future[Option[ScoreWithValue[Long]]] =
    delegateRedisClusterCommandAndLift(_.zrankWithScore(key, value)).map(Option(_).map(scoredValue => ScoreWithValue[Long](scoredValue.getScore, scoredValue.getValue)))

  override def zRevRank(key: K, value: V): Future[Option[Long]] =
    delegateRedisClusterCommandAndLift(_.zrevrank(key, value)).map(Option(_).map(Long2long))

  override def zRevRankWithScore(key: K, value: V): Future[Option[ScoreWithValue[Long]]] =
    delegateRedisClusterCommandAndLift(_.zrevrankWithScore(key, value)).map(Option(_).map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zRangeWithScores(key: K, start: Long, stop: Long): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zrangeWithScores(key, start, stop))
      .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zScan(key: K, cursor: String = InitialCursor, limit: Option[Long] = None, matchPattern: Option[String] = None): Future[ScanResults[List[ScoreWithValue[V]]]] = {
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
        delegateRedisClusterCommandAndLift(_.zscan(key, io.lettuce.core.ScanCursor.of(cursor), args)).map { scanResult =>
          ScanResults(
            cursor = scanResult.getCursor,
            finished = scanResult.isFinished,
            values = scanResult.getValues.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue))
          )
        }
      case None =>
        delegateRedisClusterCommandAndLift(_.zscan(key, io.lettuce.core.ScanCursor.of(cursor))).map { scanResult =>
          ScanResults(
            cursor = scanResult.getCursor,
            finished = scanResult.isFinished,
            values = scanResult.getValues.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue))
          )
        }
    }
  }

  override def zRandMember(key: K): Future[Option[V]] =
    delegateRedisClusterCommandAndLift(_.zrandmember(key)).map(Option(_))

  override def zRandMember(key: K, count: Long): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.zrandmember(key, count)).map(_.asScala.toList)

  override def zRandMemberWithScores(key: K): Future[Option[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zrandmemberWithScores(key)).map(Option(_).map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zRandMemberWithScores(key: K, count: Long): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zrandmemberWithScores(key, count)).map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zRem(key: K, values: V*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zrem(key, values: _*)).map(Long2long)

  override def zRemRangeByRank(key: K, start: Long, stop: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zremrangebyrank(key, start, stop)).map(Long2long)

  override def zRemRangeByScore[T: Numeric](key: K, range: ZRange[T]): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zremrangebyscore(key, toJavaNumberRange(range))).map(Long2long)

  override def zRevRange(key: K, start: Long, stop: Long): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.zrevrange(key, start, stop)).map(_.asScala.toList)

  override def zRevRangeWithScores(key: K, start: Long, stop: Long): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zrevrangeWithScores(key, start, stop))
      .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

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


  private[commands] def zAddOptionsToJava(options: Set[ZAddOptions]): ZAddArgs = {
    val args = new ZAddArgs()
    options.foreach {
      case ZAddOptions.NX => args.nx()
      case ZAddOptions.XX => args.xx()
      case ZAddOptions.CH => args.ch()
      case ZAddOptions.GT => args.gt()
      case ZAddOptions.LT => args.lt()
    }
    args
  }
}
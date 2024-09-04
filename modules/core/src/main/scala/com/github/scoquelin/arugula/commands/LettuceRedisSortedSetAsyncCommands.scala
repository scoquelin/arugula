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

  override def zDiff(keys: K*): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.zdiff(keys: _*)).map(_.asScala.toList)

  override def zDiffStore(destination: K, keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zdiffstore(destination, keys: _*)).map(Long2long)

  override def zDiffWithScores(keys: K*): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zdiffWithScores(keys: _*)).map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zLexCount(key: K, range: ZRange[V]): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.zlexcount(key, LettuceRedisSortedSetAsyncCommands.toJavaRange(range))).map(Long2long)
  }

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

  override def zMScore(key: K, members: V*): Future[List[Option[Double]]] = {
    delegateRedisClusterCommandAndLift(_.zmscore(key, members: _*)).map(_.asScala.map(Option(_).map(Double2double)).toList)
  }

  override def zRange(key: K, start: Long, stop: Long): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.zrange(key, start, stop)).map(_.asScala.toList)

  override def zRangeStore(destination: K, key: K, start: Long, stop: Long): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.zrangestore(destination, key, Range.create(start, stop))).map(Long2long)
  }

  override def zRangeStoreByLex(destination: K, key: K, range: ZRange[V], limit: Option[RangeLimit] = None): Future[Long] = {
    val args = limit match {
      case Some(rangeLimit) => io.lettuce.core.Limit.create(rangeLimit.offset, rangeLimit.count)
      case None => io.lettuce.core.Limit.unlimited()
    }
    delegateRedisClusterCommandAndLift(_.zrangestorebylex(destination, key, LettuceRedisSortedSetAsyncCommands.toJavaRange(range), args)).map(Long2long)
  }

  override def zRangeStoreByScore[T: Numeric](destination: K, key: K, range: ZRange[T], limit: Option[RangeLimit] = None): Future[Long] = {
    val limitArgs = limit match {
      case Some(rangeLimit) => Limit.create(rangeLimit.offset, rangeLimit.count)
      case None => Limit.unlimited()
    }
    delegateRedisClusterCommandAndLift(_.zrangestorebyscore(destination, key, toJavaNumberRange(range), limitArgs)).map(Long2long)
  }

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

  override def zRevRangeByLex(key: K,
    range: ZRange[V],
    limit: Option[RangeLimit] = None): Future[List[V]] = {
    val args = limit match {
      case Some(rangeLimit) => io.lettuce.core.Limit.create(rangeLimit.offset, rangeLimit.count)
      case None => io.lettuce.core.Limit.unlimited()
    }
    delegateRedisClusterCommandAndLift(_.zrevrangebylex(key, LettuceRedisSortedSetAsyncCommands.toJavaRange(range), args)).map(_.asScala.toList)
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

  override def zInter(keys: K*): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.zinter(keys: _*)).map(_.asScala.toList)

  override def zInter(args: RedisSortedSetAsyncCommands.AggregationArgs, keys: K*): Future[List[V]] = {
    delegateRedisClusterCommandAndLift(_.zinter(LettuceRedisSortedSetAsyncCommands.aggregationArgsToJava(args), keys: _*)).map(_.asScala.toList)
  }

  override def zInterCard(keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zintercard(keys: _*)).map(Long2long)

  override def zInterStore(destination: K, keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zinterstore(destination, keys: _*)).map(Long2long)

  override def zInterStore(destination: K, args: RedisSortedSetAsyncCommands.AggregationArgs, keys: K*): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.zinterstore(destination, LettuceRedisSortedSetAsyncCommands.aggregationArgsToJavaStore(args), keys: _*)).map(Long2long)
  }

  override def zInterWithScores(keys: K*): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zinterWithScores(keys: _*)).map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zInterWithScores(args: RedisSortedSetAsyncCommands.AggregationArgs, keys: K*): Future[List[ScoreWithValue[V]]] = {
    delegateRedisClusterCommandAndLift(_.zinterWithScores(LettuceRedisSortedSetAsyncCommands.aggregationArgsToJava(args), keys: _*))
      .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))
  }

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

  override def zRangeByLex(key: K, range: ZRange[V], limit: Option[RangeLimit] = None): Future[List[V]] = {
    val args = limit match {
      case Some(rangeLimit) => io.lettuce.core.Limit.create(rangeLimit.offset, rangeLimit.count)
      case None => io.lettuce.core.Limit.unlimited()
    }
    delegateRedisClusterCommandAndLift(_.zrangebylex(key, LettuceRedisSortedSetAsyncCommands.toJavaRange(range), args)).map(_.asScala.toList)
  }

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

  override def zRemRangeByLex(key: K, range: ZRange[V]): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zremrangebylex(key, LettuceRedisSortedSetAsyncCommands.toJavaRange(range))).map(Long2long)

  override def zRemRangeByRank(key: K, start: Long, stop: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zremrangebyrank(key, start, stop)).map(Long2long)

  override def zRemRangeByScore[T: Numeric](key: K, range: ZRange[T]): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zremrangebyscore(key, toJavaNumberRange(range))).map(Long2long)

  override def zRevRange(key: K, start: Long, stop: Long): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.zrevrange(key, start, stop)).map(_.asScala.toList)

  override def zRevRangeStore(destination: K, key: K, start: Long, stop: Long): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zrevrangestore(destination, key, Range.create(start, stop))).map(Long2long)

  override def zRevRangeStoreByLex(destination: K, key: K, range: ZRange[V], limit: Option[RangeLimit]): Future[Long] = {
    val args = limit match {
      case Some(rangeLimit) => io.lettuce.core.Limit.create(rangeLimit.offset, rangeLimit.count)
      case None => io.lettuce.core.Limit.unlimited()
    }
    delegateRedisClusterCommandAndLift(_.zrevrangestorebylex(destination, key, LettuceRedisSortedSetAsyncCommands.toJavaRange(range), args)).map(Long2long)
  }

  override def zRevRangeStoreByScore[T: Numeric](destination: K,
    key: K,
    range: ZRange[T],
    limit: Option[RangeLimit]): Future[Long] = {
    val limitArgs = limit match {
      case Some(rangeLimit) => Limit.create(rangeLimit.offset, rangeLimit.count)
      case None => Limit.unlimited()
    }
    delegateRedisClusterCommandAndLift(_.zrevrangestorebyscore(destination, key, toJavaNumberRange(range), limitArgs)).map(Long2long)
  }

  override def zRevRangeWithScores(key: K, start: Long, stop: Long): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zrevrangeWithScores(key, start, stop))
      .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zUnion(keys: K*): Future[List[V]] =
    delegateRedisClusterCommandAndLift(_.zunion(keys: _*)).map(_.asScala.toList)

  override def zUnion(args: RedisSortedSetAsyncCommands.AggregationArgs, keys: K*): Future[List[V]] = {
    delegateRedisClusterCommandAndLift(_.zunion(LettuceRedisSortedSetAsyncCommands.aggregationArgsToJava(args), keys: _*)).map(_.asScala.toList)
  }

  override def zUnionWithScores(keys: K*): Future[List[ScoreWithValue[V]]] =
    delegateRedisClusterCommandAndLift(_.zunionWithScores(keys: _*))
      .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))

  override def zUnionWithScores(args: RedisSortedSetAsyncCommands.AggregationArgs, keys: K*): Future[List[ScoreWithValue[V]]] = {
    delegateRedisClusterCommandAndLift(_.zunionWithScores(LettuceRedisSortedSetAsyncCommands.aggregationArgsToJava(args), keys: _*))
      .map(_.asScala.toList.map(scoredValue => ScoreWithValue(scoredValue.getScore, scoredValue.getValue)))
  }

  override def zUnionStore(destination: K, keys: K*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.zunionstore(destination, keys: _*)).map(Long2long)

  override def zUnionStore(destination: K, args: RedisSortedSetAsyncCommands.AggregationArgs, keys: K*): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.zunionstore(destination, LettuceRedisSortedSetAsyncCommands.aggregationArgsToJavaStore(args), keys: _*)).map(Long2long)
  }

}

private[arugula] object LettuceRedisSortedSetAsyncCommands{
  private[arugula] def toJavaNumberRange[T: Numeric](range: ZRange[T]): io.lettuce.core.Range[java.lang.Number] = {
    io.lettuce.core.Range.from(toJavaNumberBoundary(range.lower), toJavaNumberBoundary(range.upper))
  }

  private[commands] def toJavaNumberBoundary[T: Numeric](boundary: RedisSortedSetAsyncCommands.ZRange.Boundary[T]): io.lettuce.core.Range.Boundary[java.lang.Number] = {
    def toJavaNumber(t: T): java.lang.Number = t match {
      case b: Byte => b
      case s: Short => s
      case i: Int => i
      case l: Long => l
      case f: Float => f
      case _ => implicitly[Numeric[T]].toDouble(t)
    }
    boundary.value match {
      case Some(value) if boundary.inclusive => io.lettuce.core.Range.Boundary.including(toJavaNumber(value))
      case Some(value) => io.lettuce.core.Range.Boundary.excluding(toJavaNumber(value))
      case None => io.lettuce.core.Range.Boundary.unbounded[java.lang.Number]()
    }
  }

  private [arugula] def toJavaRange[T](range: ZRange[T]): io.lettuce.core.Range[T] = {
    io.lettuce.core.Range.from[T](toJavaBoundary(range.lower), toJavaBoundary(range.upper))
  }

  private [commands] def toJavaBoundary[T](boundary: RedisSortedSetAsyncCommands.ZRange.Boundary[T]): io.lettuce.core.Range.Boundary[T] = {
    boundary.value match {
      case Some(value) if boundary.inclusive => io.lettuce.core.Range.Boundary.including(value)
      case Some(value) => io.lettuce.core.Range.Boundary.excluding(value)
      case None => io.lettuce.core.Range.Boundary.unbounded[T]()
    }
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

  private[commands] def aggregationArgsToJavaStore(options: RedisSortedSetAsyncCommands.AggregationArgs): io.lettuce.core.ZStoreArgs = {
    val args = options.aggregate match {
      case RedisSortedSetAsyncCommands.Aggregate.Sum => io.lettuce.core.ZStoreArgs.Builder.sum()
      case RedisSortedSetAsyncCommands.Aggregate.Min => io.lettuce.core.ZStoreArgs.Builder.min()
      case RedisSortedSetAsyncCommands.Aggregate.Max => io.lettuce.core.ZStoreArgs.Builder.max()
    }
    // add weights to args. the args is a mutable java object so we can just add the weights to it
    if(options.weights.nonEmpty){
      args.weights(options.weights.map(_.doubleValue()): _*)
    }
    args
  }

  private[commands] def aggregationArgsToJava(options: RedisSortedSetAsyncCommands.AggregationArgs): io.lettuce.core.ZAggregateArgs = {
    val args = options.aggregate match {
      case RedisSortedSetAsyncCommands.Aggregate.Sum => io.lettuce.core.ZAggregateArgs.Builder.sum()
      case RedisSortedSetAsyncCommands.Aggregate.Min => io.lettuce.core.ZAggregateArgs.Builder.min()
      case RedisSortedSetAsyncCommands.Aggregate.Max => io.lettuce.core.ZAggregateArgs.Builder.max()
    }
    // add weights to args. the args is a mutable java object so we can just add the weights to it
    if(options.weights.nonEmpty){
      args.weights(options.weights.map(_.doubleValue()): _*)
    }
    args
  }
}
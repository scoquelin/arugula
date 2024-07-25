package com.github.scoquelin.arugula.api.commands

import com.github.scoquelin.arugula.api.commands.RedisKeyAsyncCommands.ScanCursor
import com.github.scoquelin.arugula.api.commands.RedisSortedSetAsyncCommands.{RangeLimit, ScanCursorWithScoredValues, ScoreWithValue, ZAddOptions, ZRange}

import scala.concurrent.Future

trait RedisSortedSetAsyncCommands[K, V] {
  def zAdd(key: K, args: Option[ZAddOptions], values: ScoreWithValue[V]*): Future[Long]
  def zPopMin(key: K, count: Long): Future[List[ScoreWithValue[V]]]
  def zPopMax(key: K, count: Long): Future[List[ScoreWithValue[V]]]
  def zRangeWithScores(key: K, start: Long, stop: Long): Future[List[ScoreWithValue[V]]]
  def zRangeByScore[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit]): Future[List[V]]
  def zRevRangeByScore[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit]): Future[List[V]]
  def zScan(key: K, cursor: ScanCursor = ScanCursor.Initial, limit: Option[Long] = None, matchPattern: Option[String] = None): Future[ScanCursorWithScoredValues[V]]
  def zRem(key: K, values: V*): Future[Long]
  def zRemRangeByRank(key: K, start: Long, stop: Long): Future[Long]
  def zRemRangeByScore[T: Numeric](key: K, range: ZRange[T]): Future[Long]
}

object RedisSortedSetAsyncCommands {
  sealed trait ZAddOptions
  object ZAddOptions {
    case object NX extends ZAddOptions

    case object XX extends ZAddOptions

    case object LT extends ZAddOptions

    case object GT extends ZAddOptions

    case object CH extends ZAddOptions
  }

  final case class ScoreWithValue[V](score: Double, value: V)
  final case class ZRange[T](start: T, end: T)
  final case class RangeLimit(offset: Long, count: Long)
  final case class ScanCursorWithScoredValues[V](cursor: ScanCursor, values: List[ScoreWithValue[V]])
}
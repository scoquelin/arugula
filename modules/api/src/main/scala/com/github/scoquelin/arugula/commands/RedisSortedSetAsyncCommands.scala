package com.github.scoquelin.arugula.commands


import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands.{InitialCursor, ScanResults}

/**
 * Asynchronous commands for manipulating/querying Sorted Sets
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisSortedSetAsyncCommands[K, V] {
  import RedisSortedSetAsyncCommands._

  /**
   * Remove and return the member with the lowest score from one or more sorted sets
   * @param timeout The timeout
   * @param direction Which end of the sorted set to pop from, the min or max
   * @param keys The keys
   * @return The member removed based on the pop direction
   */
  def bzMPop(timeout: FiniteDuration, direction: SortOrder, keys: K*): Future[Option[ScoreWithKeyValue[K,V]]]

  /**
   * Remove and return up to count members from the end of one or more sorted sets based on the pop direction (min or max)
   * @param timeout The timeout
   * @param count The number of members to pop
   * @param direction Which end of the sorted set to pop from, the min or max
   * @param keys The keys
   * @return The members removed based on the pop direction
   */
  def bzMPop(timeout: FiniteDuration, count: Int, direction: SortOrder, keys: K*): Future[List[ScoreWithKeyValue[K,V]]]

  /**
   * Remove and return the member with the lowest score from one or more sorted sets
   * @param timeout The timeout
   * @param keys The keys
   * @return The member with the lowest score, or None if the sets are empty
   */
  def bzPopMin(timeout: FiniteDuration, keys: K*): Future[Option[ScoreWithKeyValue[K,V]]]

  /**
   * Remove and return the member with the highest score from one or more sorted sets
   * @param timeout The timeout
   * @param keys The keys
   * @return The member with the highest score, or None if the sets are empty
   */
  def bzPopMax(timeout: FiniteDuration, keys: K*): Future[Option[ScoreWithKeyValue[K,V]]]


  /**
   * Add one or more members to a sorted set, or update its score if it already exists
   * @param key The key
   * @param values The values to add
   * @return The number of elements added to the sorted set
   */
  def zAdd(key: K, values: ScoreWithValue[V]*): Future[Long]

  /**
   * Add one or more members to a sorted set, or update its score if it already exists
   * @param key The key
   * @param args Optional arguments
   * @param values The values to add
   * @return The number of elements added to the sorted set
   */
  def zAdd(key: K, args: ZAddOptions, values: ScoreWithValue[V]*): Future[Long] = zAdd(key, Set(args), values: _*)

  /**
   * Add one or more members to a sorted set, or update its score if it already exists
   * @param key The key
   * @param args Optional arguments
   * @param values The values to add
   * @return The number of elements added to the sorted set
   */
  def zAdd(key: K, args: Set[ZAddOptions], values: ScoreWithValue[V]*): Future[Long]

  /**
   * Add one or more members to a sorted set, or update its score if it already exists
   * @param key The key
   * @return The number of members in the sorted set
   */
  def zAddIncr(key: K, score: Double, member: V): Future[Option[Double]]

  /**
   * Add one or more members to a sorted set, or update its score if it already exists
   * @param key The key
   * @param args The arguments
   * @param values The values to add
   * @return The number of elements added to the sorted set
   */
  def zAddIncr(key: K, args: ZAddOptions, score: Double, member: V): Future[Option[Double]] = zAddIncr(key, Set(args), score, member)

  /**
   * Add one or more members to a sorted set, or update its score if it already exists
   * @param key The key
   * @param values The values to add
   * @return The number of elements added to the sorted set
   */
  def zAddIncr(key: K, args: Set[ZAddOptions], score: Double, member: V): Future[Option[Double]]

  /**
   * Get the number of members in a sorted set
   * @param key The key
   * @return The number of members in the sorted set
   */
  def zCard(key: K): Future[Long]

  /**
   * Count the members in a sorted set with scores within the given values
   * @param key The key
   * @param range The range of scores
   * @return The number of elements in the specified score range
   */
  def zCount[T: Numeric](key: K, range: ZRange[T]): Future[Long]

  /**
   * Remove and return a member from the end of one or more sorted sets based on the pop direction (min or max)
   * @param direction The direction to pop from
   *                     (min or max)
   * @param keys The keys
   * @return The member removed based on the pop direction
   */
  def zMPop(direction: SortOrder, keys: K*): Future[Option[ScoreWithKeyValue[K,V]]]

  /**
   * Remove and return up to count members from the end of one or more sorted sets based on the pop direction (min or max)
   * @param count The number of members to pop
   * @param direction The direction to pop from
   * @param keys The keys
   * @return The members removed based on the pop direction
   */
  def zMPop(count: Int, direction: SortOrder, keys: K*): Future[List[ScoreWithKeyValue[K,V]]]

  /**
   * Remove and return a member with the lowest score from a sorted set
   * @param key The key
   * @return The member with the lowest score, or None if the set is empty
   */
  def zPopMin(key: K): Future[Option[ScoreWithValue[V]]]

  /**
   * Remove and return up to count members with the lowest scores in a sorted set
   * @param key The key
   * @param count The number of members to pop
   * @return The members with the lowest scores
   */
  def zPopMin(key: K, count: Long): Future[List[ScoreWithValue[V]]]

  /**
   * Remove and return a member with the highest score from a sorted set
   * @param key The key
   * @return The member with the highest score, or None if the set is empty
   */
  def zPopMax(key: K): Future[Option[ScoreWithValue[V]]]

  /**
   * Remove and return up to count members with the highest scores in a sorted set
   * @param key The key
   * @param count The number of members to pop
   * @return The members with the highest scores
   */
  def zPopMax(key: K, count: Long): Future[List[ScoreWithValue[V]]]

  /**
   * Get the score of a member in a sorted set
   * @param key The key
   * @param value The value
   * @return The score of the member, or None if the member does not exist
   */
  def zScore(key: K, value: V): Future[Option[Double]]

  /**
   * Return a range of members in a sorted set, by index
   * @param key The key
   * @param start The start index
   * @param stop The stop index
   * @return The members in the specified range
   */
  def zRange(key: K, start: Long, stop: Long): Future[List[V]]

  /**
   * Return a range of members with scores in a sorted set, by index.
   * @param key The key
   * @param start The start index
   * @param stop The stop index
   * @return The members with scores in the specified range
   */
  def zRangeWithScores(key: K, start: Long, stop: Long): Future[List[ScoreWithValue[V]]]

  /**
   * Return a range of members in a sorted set, by score
   * @param key The key
   * @param range The range of scores
   * @param limit Optional limit
   * @return The members in the specified score range
   */
  def zRangeByScore[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit]): Future[List[V]]

  /**
   * Return a range of members in a sorted set, by score, with scores ordered from high to low
   * @param key The key
   * @param range The range of scores
   * @param limit Optional limit
   * @return The members in the specified score range
   */
  def zRangeByScoreWithScores[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit] = None): Future[List[ScoreWithValue[V]]]

  /**
   * Return a range of members in a sorted set, by score, with scores ordered from high to low
   * @param key The key
   * @param range The range of scores
   * @param limit Optional limit
   * @return The members in the specified score range
   */
  def zRevRangeByScore[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit]): Future[List[V]]

  /**
   * Return a range of members in a sorted set, by score, with scores ordered from high to low
   * @param key The key
   * @param range The range of scores
   * @param limit Optional limit
   * @return The members in the specified score range
   */
  def zRevRangeByScoreWithScores[T: Numeric](key: K, range: ZRange[T], limit: Option[RangeLimit] = None): Future[List[ScoreWithValue[V]]]

  /**
   * Increment the score of a member in a sorted set
   * @param key The key
   * @param amount The amount to increment by
   * @param value The value
   * @return The new score of the member
   */
  def zIncrBy(key: K, amount: Double, value: V): Future[Double]

  /**
   * Determine the index of a member in a sorted set
   * @param key The key
   * @param value The value
   * @return The index of the member, or None if the member does not exist
   */
  def zRank(key: K, value: V): Future[Option[Long]]

  /**
   * Determine the index of a member in a sorted set, with the score
   * @param key The key
   * @param value The value
   * @return The index of the member, or None if the member does not exist
   */
  def zRankWithScore(key: K, value: V): Future[Option[ScoreWithValue[Long]]]

  /**
   * Determine the index of a member in a sorted set, with scores ordered from high to low.
   * @param key The key.
   * @param value the member type: value.
   * @return Long integer-reply the rank of the element as an integer-reply, with the scores ordered from high to low
   *         or None if the member does not exist.
   */
  def zRevRank(key: K, value: V): Future[Option[Long]]

  /**
   * Determine the index of a member in a sorted set, with the score
   * @param key The key
   * @param value The value
   * @return The index of the member, or None if the member does not exist
   */
  def zRevRankWithScore(key: K, value: V): Future[Option[ScoreWithValue[Long]]]

  /**
   * Scan a sorted set
   * @param key The key
   * @param cursor The cursor
   * @param limit The maximum number of elements to return
   * @param matchPattern The pattern to match
   * @return The cursor and the values
   */
  def zScan(key: K, cursor: String = InitialCursor, limit: Option[Long] = None, matchPattern: Option[String] = None): Future[ScanResults[List[ScoreWithValue[V]]]]

  /**
   * Get a random member from a sorted set
   * @param key The key
   * @return A random member from the sorted set, or None if the set is empty
   */
  def zRandMember(key: K): Future[Option[V]]

  /**
   * Get multiple random members from a sorted set
   * @param key The key
   * @param count The number of members to get
   * @return A list of random members from the sorted set
   */
  def zRandMember(key: K, count: Long): Future[List[V]]

  /**
   * Get a random member from a sorted set, with the score
   * @param key The key
   * @return A random member from the sorted set, or None if the set is empty
   */
  def zRandMemberWithScores(key: K): Future[Option[ScoreWithValue[V]]]

  /**
   * Get multiple random members from a sorted set, with the score
   * @param key The key
   * @param count The number of members to get
   * @return A list of random members from the sorted set
   */
  def zRandMemberWithScores(key: K, count: Long): Future[List[ScoreWithValue[V]]]

  /**
   * Remove one or more members from a sorted set
   * @param key The key
   * @param values The values to remove
   * @return The number of members removed from the sorted set
   */
  def zRem(key: K, values: V*): Future[Long]

  /**
   * Remove all members in a sorted set with scores between the given values
   * @param key The key
   * @param start The start score
   * @param stop The stop score
   * @return The number of members removed from the sorted set
   */
  def zRemRangeByRank(key: K, start: Long, stop: Long): Future[Long]

  /**
   * Remove all members in a sorted set with scores between the given values
   * @param key The key
   * @param range The range of scores
   * @return The number of members removed from the sorted set
   */
  def zRemRangeByScore[T: Numeric](key: K, range: ZRange[T]): Future[Long]

  /**
   * Get the score of a member in a sorted set, with the score
   * @param key The key
   * @param start The start index
   * @param stop The stop index
   * @return The members with scores in the specified range
   */
  def zRevRange(key: K, start: Long, stop: Long): Future[List[V]]

  /**
   * Get the score of a member in a sorted set, with the score
   * @param key The key
   * @param start The start index
   * @param stop The stop index
   * @return The members with scores in the specified range
   */
  def zRevRangeWithScores(key: K, start: Long, stop: Long): Future[List[ScoreWithValue[V]]]
}

// TODO : Implement the following commands:
//zdiff
//zdiffstore
//zdiffWithScores
//zinter
//zintercard
//zinterWithScores
//zinterstore
//zlexcount
//zlexcount
//zmscore
//zmpop
//zrandmemberWithScores
//zrangebylex
//zrangestore
//zrangestorebylex
//zrangestorebyscore
//zremrangebylex
//zrevrangebylex
//zrevrangestore
//zrevrangestorebylex
//zrevrangestorebyscore
//zunion
//zunionWithScores
//zunionstore

/**
 * Companion object for RedisSortedSetAsyncCommands
 */
object RedisSortedSetAsyncCommands {

  sealed trait ZAddOptions
  object ZAddOptions {

    /**
     * Takes a varargs of ZAddOptions and returns a Set of ZAddOptions.
     * Useful for passing multiple options to a command.
     * @param options The options
     * @return The set of options
     */
    def apply(options: ZAddOptions*): Set[ZAddOptions] = options.toSet

    /**
     * Only add new elements
     */
    case object NX extends ZAddOptions

    /**
     * Only update elements that already exist
     */
    case object XX extends ZAddOptions

    /**
     * Only update elements that already exist and return the new score
     */
    case object LT extends ZAddOptions

    /**
     * Only add new elements and return the new score
     */
    case object GT extends ZAddOptions

    /**
     * Only update elements that already exist and return the new score
     */
    case object CH extends ZAddOptions
  }

  final case class ScoreWithValue[V](score: Double, value: V)
  final case class ScoreWithKeyValue[K, V](score: Double, key: K, value: V)
  final case class ZRange[T](start: T, end: T)
  final case class RangeLimit(offset: Long, count: Long)

  sealed trait SortOrder

  object SortOrder {
    case object Min extends SortOrder
    case object Max extends SortOrder
  }
}
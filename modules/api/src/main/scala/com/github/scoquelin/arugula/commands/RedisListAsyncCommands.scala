package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.TimeUnit

/**
 * Asynchronous commands for manipulating/querying Lists (ordered collections of elements)
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisListAsyncCommands[K, V] {

  def blPop(
    timeout: FiniteDuration,
    keys: K*
  ): Future[Option[(K, V)]]

  /**
   * Atomically returns and removes the first/ last element (head/ tail depending on the where from argument)
   * of the list stored at source, and pushes the element at the first/ last element
   * (head/ tail depending on the whereto argument) of the list stored at destination.
   * When source is empty, Redis will block the connection until another client pushes to it
   * or until timeout is reached.
   * @param source The source list
   * @param destination The destination list
   * @param sourceSide The side of the source list to pop from
   * @param destinationSide The side of the destination list to push to
   * @param timeout The timeout as a finite duration. Zero is infinite wait
   * @return The element that was moved, or None if the source list is empty
   */
  def blMove(
    source: K,
    destination: K,
    sourceSide: RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Right,
    destinationSide: RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Left,
    timeout: FiniteDuration = new FiniteDuration(0, TimeUnit.MILLISECONDS), // zero is infinite wait
  ): Future[Option[V]]

  /**
   * Remove and get the first/ last elements in a list, or block until one is available.
   * @param keys The source lists
   * @param direction The side of the source list to pop from
   * @param count The number of elements to pop
   * @param timeout The timeout in seconds
   * @return The element that was moved, or None if the source list is empty
   */
  def blMPop(
    keys: List[K],
    direction:RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Left,
    count: Int = 1,
    timeout: FiniteDuration = FiniteDuration(0, TimeUnit.MILLISECONDS),
  ): Future[Option[(K, List[V])]]

  /**
   * Remove and get the last element in a list, or block until one is available.
   * @param timeout The timeout in seconds
   * @param keys The source lists
   * @return The element that was moved, or None if the source list is empty
   */
  def brPop(
    timeout: FiniteDuration,
    keys: K*
  ): Future[Option[(K, V)]]

  /**
   * Pop an element from a list, push it to another list and return it; or block until one is available.
   * @param timeout The timeout in seconds
   * @param source The source list
   * @param destination The destination list
   * @return The element that was moved, or None if the source list is empty
   */
  def brPopLPush(timeout: FiniteDuration, source: K, destination: K): Future[Option[V]]

  /**
   * Insert an element before or after another element in a list.
   * @param key The key
   * @param before Whether to insert before or after the pivot
   * @param pivot The pivot element
   * @param value The value to insert
   * @return The length of the list after the insert operation
   */
  def lInsert(
    key: K,
    before: Boolean,
    pivot: V,
    value: V
  ): Future[Long]

  /**
   * Remove and get the first/ last elements in a list, or block until one is available.
   * @param keys The source lists
   * @param direction The side of the source list to pop from
   * @param count The number of elements to pop
   * @return The element that was moved, or None if the source list is empty
   */
  def lMPop(
    keys: List[K],
    direction:RedisListAsyncCommands.Side = RedisListAsyncCommands.Side.Left,
    count: Int = 1,
  ): Future[Option[(K, List[V])]]

  /**
   * Prepend one or multiple values to a list.
   * @param key The key
   * @param values The values to add
   * @return The length of the list after the push operation
   */
  def lPush(key: K, values: V*): Future[Long]

  /**
   * Prepend a value to a list, only if the list exists.
   * @param key The key
   * @param values The values to add
   * @return The length of the list after the push operation
   */
  def lPushX(key: K, values: V*): Future[Long]

  /**
   * Append one or multiple values to a list.
   * @param key The key
   * @param values The values to add
   * @return The length of the list after the push operation
   */
  def rPush(key: K, values: V*): Future[Long]

  /**
   * Append a value to a list, only if the list exists.
   * @param key The key
   * @param values The values to add
   * @return The length of the list after the push operation
   */
  def rPushX(key: K, values: V*): Future[Long]

  /**
   * Remove and get the first element in a list
   * @param key The key
   * @return The element that was removed, or None if the list is empty
   */
  def lPop(key: K): Future[Option[V]]

  /**
   * Remove and get the last element in a list
   * @param key The key
   * @return The element that was removed, or None if the list is empty
   */
  def rPop(key: K): Future[Option[V]]

  /**
   * Remove the last element in a list, append it to another list and return it
   * @param source The source key
   * @param destination The destination key
   * @return The element that was moved, or None if the source list is empty
   */
  def rPopLPush(source: K, destination: K): Future[Option[V]]

  /**
   * Get a range of elements from a list
   * @param key The key
   * @param start The start index
   * @param end The end index
   * @return The list of elements in the specified range
   */
  def lRange(key: K, start: Long, end: Long): Future[List[V]]

  /**
   * Insert an element before or after another element in a list
   * @param source The source key
   * @param destination The destination key
   * @param sourceSide The side of the source list to pop from
   * @param destinationSide The side of the destination list to push to
   * @return The element that was moved, or None if the source list is empty
   */
  def lMove(
    source: K,
    destination: K,
    sourceSide: RedisListAsyncCommands.Side,
    destinationSide: RedisListAsyncCommands.Side
  ): Future[Option[V]]

  /**
   * Get the index of an element in a list
   * @param key The key
   * @param value The value
   * @return The index of the element, or None if the element is not in the list
   */
  def lPos(key: K, value: V): Future[Option[Long]]

  /**
   * Get the length of a list
   * @param key The key
   * @return The length of the list
   */
  def lLen(key: K): Future[Long]

  /**
   * Remove and get the first element in a list
   * @param key The key
   * @return The number of elements that were removed
   */
  def lRem(key: K, count: Long, value: V): Future[Long]

  /**
   * Set the value of an element in a list by its index.
   * @param key The key
   * @param index The index
   * @param value The value
   */
  def lSet(key: K, index: Long, value: V): Future[Unit]

  /**
   * Trim a list to the specified range
   * @param key The key
   * @param start The start index
   * @param end The end index
   */
  def lTrim(key: K, start: Long, end: Long): Future[Unit]

  /**
   * Set the value of a list element by index
   * @param key The key
   * @param index The index
   * @return The value of the element at the specified index, or None if the index is out of range
   */
  def lIndex(key: K, index: Long): Future[Option[V]]
}

object RedisListAsyncCommands {

  /**
   * The side of a list to pop from
   */
  sealed trait Side

  object Side {
    /**
     * The left side of the list
     */
    case object Left extends Side

    /**
     * The right side of the list
     */
    case object Right extends Side

  }
}

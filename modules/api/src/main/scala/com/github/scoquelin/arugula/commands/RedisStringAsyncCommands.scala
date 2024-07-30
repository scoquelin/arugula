package com.github.scoquelin.arugula.commands

import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Asynchronous commands for manipulating/querying Strings
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisStringAsyncCommands[K, V] {

  /**
   * Append a value to a key.
   * @param key The key
   * @param value The value
   * @return The length of the string after the append operation
   */
  def append(key: K, value: V): Future[Long]

  /**
   * Get the value of a key.
   * @param key The key
   * @return The value of the key, or None if the key does not exist
   */
  def get(key: K): Future[Option[V]]

  /**
   * Get the value of key and delete the key.
   * @param key The key
   * @return The value of the key prior to deletion, or None if the key did not exist
   */
  def getDel(key: K): Future[Option[V]]


  /**
   * Get the value of a key and set its expiration.
   * @param key The key
   * @param expiresIn The expiration time
   * @return
   */
  def getEx(key: K, expiresIn: FiniteDuration): Future[Option[V]]

  /**
   * Get the value of a key and set a new value.
   * @param key The key
   * @param start The start index
   * @param end The end index
   * @return The value of the key
   */
  def getRange(key: K, start: Long, end: Long): Future[Option[V]]

  /**
   * Get the value of a key and set a new value.
   * @param key The key
   * @param value The value
   * @return The old value of the key
   */
  def getSet(key: K, value: V): Future[Option[V]]

  /**
   * Get the values of all the given keys.
   * @param keys The keys
   * @return The values of the keys, in the same order as the keys
   */
  def mGet(keys: K*): Future[ListMap[K, Option[V]]]

  /**
   * Set multiple keys to multiple values.
   * @param keyValues The key-value pairs
   * @return Unit
   */
  def mSet(keyValues: Map[K, V]): Future[Unit]


  /**
   * Set multiple keys to multiple values, only if none of the keys exist.
   * @param keyValues The key-value pairs
   * @return true if all keys were set, false otherwise
   */
  def mSetNx(keyValues: Map[K, V]): Future[Boolean]

  /**
   * Set the value of a key.
   * @param key The key
   * @param value The value
   * @return Unit
   */
  def set(key: K, value: V): Future[Unit]

  /**
   * Set the value and expiration of a key.
   * @param key     the key.
   * @param value   the value.
   * @param expiresIn the expiration time.
   */
  def setEx(key: K, value: V, expiresIn: FiniteDuration): Future[Unit]

  /**
   * Set the value of a key, only if the key does not exist.
   * @param key The key
   * @param value The value
   * @return true if the key was set, false otherwise
   */
  def setNx(key: K, value: V): Future[Boolean]

  /**
   * Overwrite part of a string at key starting at the specified offset.
   * @param key The key
   * @param offset The offset
   * @param value The value
   * @return The length of the string after the append operation
   */
  def setRange(key: K, offset: Long, value: V): Future[Long]

  /**
   * Get the length of the value stored in a key.
   * @param key The key
   * @return The length of the string at key, or 0 if the key does not exist
   */
  def strLen(key: K): Future[Long]

  /**
   * Increment the integer value of a key by one.
   * @param key The key
   * @return The value of the key after the increment
   */
  def incr(key: K): Future[Long]

  /**
   * Increment the integer value of a key by the given number.
   * @param key The key
   * @param increment The increment value
   * @return The value of the key after the increment
   */
  def incrBy(key: K, increment: Long): Future[Long]

  /**
   * Increment the float value of a key by the given amount.
   * @param key The key
   * @param increment The increment value
   * @return The value of the key after the increment
   */
  def incrByFloat(key: K, increment: Double): Future[Double]

  /**
   * Decrement the integer value of a key by one.
   * @param key The key
   * @return The value of the key after the decrement
   */
  def decr(key: K): Future[Long]

  /**
   * Decrement the integer value of a key by the given number.
   * @param key The key
   * @param decrement The decrement value
   * @return The value of the key after the decrement
   */
  def decrBy(key: K, decrement: Long): Future[Long]


  /*** commands that are not yet implemented ***/
  // def bitCount(key: K, start: Option[Long] = None, end: Option[Long] = None): Future[Long]
  // def bitOpAnd(destination: K, keys: K*): Future[Long]
  // def bitOpOr(destination: K, keys: K*): Future[Long]
  // def bitOpXor(destination: K, keys: K*): Future[Long]
  // def bitOpNot(destination: K, key: K): Future[Long]
  // def bitPos(key: K, bit: Boolean, start: Option[Long] = None, end: Option[Long] = None): Future[Long]
  // def bitField(key: K, command: String, offset: Long, value: Option[Long] = None): Future[Long]
  // def strAlgoLcs(keys: K*): Future[Option[V]]
  // def getBit(key: K, offset: Long): Future[Boolean]
  // def setBit(key: K, offset: Long, value: Boolean): Future[Boolean]

}
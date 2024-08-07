package com.github.scoquelin.arugula.commands

import scala.collection.immutable.ListMap
import scala.concurrent.Future

import com.github.scoquelin.arugula.commands.RedisKeyAsyncCommands.ScanCursor

/**
 * Asynchronous commands for manipulating/querying Hashes (key/value pairs)
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisHashAsyncCommands[K, V] {
  /**
   * Delete one or more hash fields
   * @param key The key
   * @param fields The fields to delete
   * @return The number of fields that were removed from the hash
   */
  def hDel(key: K, fields: K*): Future[Long]

  /**
   * Determine if a hash field exists
   * @param key The key
   * @param field The field
   * @return True if the field exists, false otherwise
   */
  def hExists(key: K, field: K): Future[Boolean]

  /**
   * Get the value of a hash field
   * @param key The key
   * @param field The field
   * @return The value of the field, or None if the field does not exist
   */
  def hGet(key: K, field: K): Future[Option[V]]

  /**
   * get all the keys in a hash
   * @param key The key
   * @return A list of all the keys in the hash
   */
  def hKeys(key: K): Future[List[K]]

  /**
   * Get the number of fields in a hash
   * @param key The key
   * @return The number of fields in the hash
   */
  def hLen(key: K): Future[Long]

  /**
   * Get the values of all the fields in a hash
   * @param key The key
   * @return A list of all the values in the hash
   */
  def hGetAll(key: K): Future[Map[K, V]]

  /**
   * Set the value of a hash field
   * @param key The key
   * @param field The field
   * @param value The value
   * @return True if the field is new, false if it was updated
   */
  def hSet(key: K, field: K, value: V): Future[Boolean]

  /**
   * Get the values of multiple hash fields
   * @param key The key
   * @param fields The fields
   * @return A map of field -> value for each field that exists or None if the field does not exist
   */
  def hMGet(key: K, fields: K*): Future[ListMap[K, Option[V]]]

  /**
   * Set the values of multiple hash fields
   * @param key The key
   * @param values A map of field -> value
   */
  def hMSet(key: K, values: Map[K, V]): Future[Unit]

  /**
   * Set the value of a hash field, only if the field does not exist
   * @param key The key
   * @param field The field
   * @param value The value
   * @return True if the field was set, false if the field already existed
   */
  def hSetNx(key: K, field: K, value: V): Future[Boolean]

  /**
   * Increment the integer value of a hash field by the given number
   * @param key The key
   * @param field The field
   * @param amount The amount to increment by
   * @return The new value of the field
   */
  def hIncrBy(key: K, field: K, amount: Long): Future[Long]

  /**
   * Get a random field from a hash
   * @param key The key
   * @return A random field from the hash, or None if the hash is empty
   */
  def hRandField(key: K): Future[Option[K]]

  /**
   * Get multiple random fields from a hash
   * @param key The key
   * @param count The number of fields to get
   * @return A list of random fields from the hash
   */
  def hRandField(key: K, count: Long): Future[List[K]]

  /**
   * Get a random field and its value from a hash
   * @param key The key
   * @return A random field and its value from the hash, or None if the hash is empty
   */
  def hRandFieldWithValues(key: K): Future[Option[(K, V)]]

  /**
   * Get multiple random fields and their values from a hash
   * @param key The key
   * @param count The number of fields to get
   * @return A map of random fields and their values from the hash
   */
  def hRandFieldWithValues(key: K, count: Long): Future[Map[K, V]]

  /**
   * scan the fields of a hash, returning the cursor and a map of field -> value
   * Repeat calls with the returned cursor to get all fields until the cursor is finished
   * @param key The key
   * @param cursor The cursor
   * @param limit The maximum number of fields to return
   * @param matchPattern A glob-style pattern to match fields against
   * @return The next cursor and a map of field -> value
   */
  def hScan(key: K,  cursor: ScanCursor = ScanCursor.Initial, limit: Option[Long] = None, matchPattern: Option[String] = None): Future[(ScanCursor, Map[K, V])]

  /**
   * Get the length of a hash field value
   * @param key The key
   * @param field The field
   * @return The length of the field value
   */
  def hStrLen(key: K, field: K): Future[Long]

  /**
   * Get the values of all the fields in a hash
   * @param key The key
   * @return A list of all the values in the hash
   */
  def hVals(key: K): Future[List[V]]
}

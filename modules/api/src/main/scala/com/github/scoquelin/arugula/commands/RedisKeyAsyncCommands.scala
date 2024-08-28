package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands.{InitialCursor, ScanResults}

import java.time.Instant

/**
 * Asynchronous commands for manipulating/querying Keys
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisKeyAsyncCommands[K, V] {

  /**
   * Copy a key to another key
   * @param srcKey The key to copy
   * @param destKey The key to copy to
   * @return True if the key was copied, false otherwise
   */
  def copy(srcKey: K, destKey: K): Future[Boolean]

  /**
   * Copy a key to another key with additional arguments
   * @param srcKey The key to copy
   * @param destKey The key to copy to
   * @param args Additional arguments for the copy operation
   */
  def copy(srcKey: K, destKey: K, args: RedisKeyAsyncCommands.CopyArgs): Future[Unit]

  /**
   * Delete one or more keys
   * @param key The key(s) to delete
   * @return The number of keys that were removed
   */
  def del(key: K*): Future[Long]

  /**
   * Unlink one or more keys. (non-blocking version of DEL)
   * @param key The key(s) to unlink
   * @return The number of keys that were unlinked
   */
  def unlink(key: K*): Future[Long]

  /**
   * Serialize a key
   * @param key The key to serialize
   * @return The serialized value of the key
   */
  def dump(key: K): Future[Array[Byte]]

  /**
   * Determine if a key exists
   * @param key The key to check
   * @return True if the key exists, false otherwise
   */
  def exists(key: K*): Future[Boolean]

  /**
   * Set a key's time to live. The key will be automatically deleted after the timeout.
   * Implementations may round the timeout to the nearest second if necessary
   * but could set a more precise timeout if the underlying Redis client supports it.
   * @param key The key to set the expiration for
   * @param expiresIn The duration until the key expires
   * @return True if the timeout was set, false otherwise
   */
  def expire(key: K, expiresIn: FiniteDuration): Future[Boolean]

  /**
   * Set the expiration for a key as an Instant
   * @param key The key to set the expiration for
   * @param timestamp The point in time when the key should expire
   * @return True if the timeout was set, false otherwise
   */
  def expireAt(key: K, timestamp: Instant): Future[Boolean]

  /**
   * Get the time to live for a key as an Instant
   * @param key The key to get the expiration for
   * @return The time to live as a point in time, or None if the key does not exist or does not have an expiration
   */
  def expireTime(key: K): Future[Option[Instant]]

  /**
   * Find all keys matching the given pattern
   * To match all keys, use "*"
   * @param pattern The pattern to match
   * @return The keys that match the pattern
   */
  def keys(pattern: K): Future[List[K]]

  /**
   * Move a key to a different database
   * @param key The key to move
   * @param db The database to move the key to
   * @return True if the key was moved, false otherwise
   */
  def move(key: K, db: Int): Future[Boolean]

  /**
   * Rename a key
   * @param key The key to rename
   * @param newKey The new name for the key
   */
  def rename(key: K, newKey: K): Future[Unit]

  /**
   * Rename a key, but only if the new key does not already exist
   * @param key The key to rename
   * @param newKey The new name for the key
   * @return True if the key was renamed, false otherwise
   */
  def renameNx(key: K, newKey: K): Future[Boolean]

  /**
   * Restore a key from its serialized form
   * @param key The key to restore
   * @param serializedValue The serialized value of the key
   * @param args Additional arguments for the restore operation
   */
  def restore(key: K, serializedValue: Array[Byte], args: RedisKeyAsyncCommands.RestoreArgs = RedisKeyAsyncCommands.RestoreArgs()): Future[Unit]

  /**
   * Scan the keyspace
   * @param cursor The cursor to start scanning from
   * @param matchPattern An optional pattern to match keys against
   * @param limit An optional limit on the number of keys to return
   * @return The keys that were scanned
   */
  def scan(cursor: String = InitialCursor, matchPattern: Option[String] = None, limit: Option[Int] = None): Future[ScanResults[List[K]]]

  /**
   * Get the time to live for a key.
   * Implementations may return a more precise time to live if the underlying Redis client supports it.
   * Rather than expose the underlying Redis client's API, this method returns a FiniteDuration which can
   * be rounded to the nearest second if necessary.
   * @param key The key to get the expiration for
   * @return The time to live, or None if the key does not exist or does not have an expiration
   */
  def ttl(key: K): Future[Option[FiniteDuration]]

  /**
   * Alters the last access time of a key(s). A key is ignored if it does not exist.
   * @param key The key(s) to touch
   * @return The number of keys that were touched
   */
  def touch(key: K*): Future[Long]

  /**
   * Get the type of a key
   * @param key The key to get the type of
   * @return The type of the key
   */
  def `type`(key: K): Future[String]
}

object RedisKeyAsyncCommands {
  case class CopyArgs(replace: Boolean = false, destinationDb: Option[Int] = None)

  case class RestoreArgs(
    replace: Boolean = false,
    idleTime: Option[FiniteDuration] = None,
    ttl: Option[FiniteDuration] = None,
    absTtl: Option[Instant] = None,
    frequency: Option[Long] = None,
  ){
    def isEmpty: Boolean = !replace && idleTime.isEmpty && frequency.isEmpty && ttl.isEmpty && absTtl.isEmpty
  }
}

// Commands to be Implemented:
//migrate
//objectEncoding
//objectFreq
//objectIdletime
//objectRefcount
//randomkey
//sort
//sortReadOnly
//sortStore

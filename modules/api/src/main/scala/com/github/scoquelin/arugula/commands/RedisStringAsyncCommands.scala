package com.github.scoquelin.arugula.commands

import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import com.github.scoquelin.arugula.commands.RedisStringAsyncCommands.BitFieldCommand

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
   * Count the number of set bits (population counting) in a string.
   */
  def bitCount(key: K): Future[Long]

  /**
   * Count the number of set bits (population counting) in a string.
   * @param key The key
   * @param start The start index
   * @param end The end index
   * @return The number of bits set to 1
   */
  def bitCount(key: K, start: Long, end: Long): Future[Long]

  /**
   * Perform arbitrary bitfield integer operations on strings.
   * Use the BitFieldCommand object convenience methods to create the commands.
   * Example:
   * {{{
   *  val commands = Seq(
   *    BitFieldCommand.set(BitFieldDataType.Unsigned(8), 1),
   *    BitFieldCommand.incrBy(BitFieldDataType.Unsigned(8), 1),
   *    BitFieldCommand.get(BitFieldDataType.Unsigned(8)),
   *    BitFieldCommand.overflowSat(BitFieldDataType.Unsigned(8))
   *  )
   *  bitField("key", commands)
   *  }}}
   * @param key The key
   * @param commands The commands
   * @return The length of the string stored in the destination key
   */
  def bitField(key: K, commands: Seq[BitFieldCommand]): Future[Seq[Long]]

  /**
   * Perform bitwise AND between strings.
   * @param destination The destination key
   * @param keys The keys
   * @return The length of the string stored in the destination key
   */
  def bitOpAnd(destination: K, keys: K*): Future[Long]


  /**
   * Perform bitwise OR between strings.
   * @param destination The destination key
   * @param keys The keys
   * @return The length of the string stored in the destination key
   */
  def bitOpOr(destination: K, keys: K*): Future[Long]

  /**
   * Perform bitwise XOR between strings.
   * @param destination The destination key
   * @param keys The keys
   * @return The length of the string stored in the destination key
   */
  def bitOpXor(destination: K, keys: K*): Future[Long]


  /**
   * Perform bitwise NOT between strings.
   * @param destination The destination key
   * @param key The key
   * @return The length of the string stored in the destination key
   */
  def bitOpNot(destination: K, key: K): Future[Long]

  /**
   * Find first bit set or clear in a string.
   * @param key The key
   * @param state The state
   * @return The command returns the position of the first bit set to 1 or 0 according to the request.
   */
  def bitPos(key: K, state:Boolean): Future[Long]

  /**
   * Find first bit set or clear in a string.
   * @param key The key
   * @param state The state
   * @param start The start index
   * @return The command returns the position of the first bit set to 1 or 0 according to the request.
   */
  def bitPos(key: K, state: Boolean, start: Long): Future[Long]

  /**
   * Find first bit set or clear in a string.
   * @param key The key
   * @param state The state
   * @param start The start index
   * @param end The end index
   * @return The command returns the position of the first bit set to 1 or 0 according to the request.
   */
  def bitPos(key: K, state: Boolean, start: Long, end: Long): Future[Long]

  /**
   * Get the bit value at offset in the string value stored at key.
   * @param key The key
   * @param offset The offset
   * @return The bit value stored at offset
   */
  def getBit(key: K, offset: Long): Future[Long]

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
   * Set or clear the bit at offset in the string value stored at key.
   * @param key The key
   * @param offset The offset
   * @param value The value
   * @return The original bit value stored at offset
   */
  def setBit(key: K, offset: Long, value: Int): Future[Long]

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
  // def strAlgoLcs(keys: K*): Future[Option[V]]
}



object RedisStringAsyncCommands {

  sealed trait BitFieldOperation

  object BitFieldOperation {
    case object Get extends BitFieldOperation
    case object Set extends BitFieldOperation
    case object Incrby extends BitFieldOperation
    case object OverflowWrap extends BitFieldOperation
    case object OverflowSat extends BitFieldOperation
    case object OverflowFail extends BitFieldOperation
  }

  sealed trait BitFieldDataType

  object BitFieldDataType {
    final case class Signed(width: Int) extends BitFieldDataType
    final case class Unsigned(width: Int) extends BitFieldDataType
  }

  final case class BitFieldCommand(operation: BitFieldOperation, dataType: BitFieldDataType, offset: Option[Int] = None, value: Option[Long] = None)

  object BitFieldCommand {
    def set(bitFieldType: BitFieldDataType, value: Long): BitFieldCommand = BitFieldCommand(BitFieldOperation.Set, bitFieldType, None, Some(value))
    def set(bitFieldType: BitFieldDataType, value: Long, offset: Int): BitFieldCommand = BitFieldCommand(BitFieldOperation.Set, bitFieldType, Some(offset), Some(value))
    def get(bitFieldType: BitFieldDataType): BitFieldCommand = BitFieldCommand(BitFieldOperation.Get, bitFieldType)
    def get(bitFieldType: BitFieldDataType, offset: Int): BitFieldCommand = BitFieldCommand(BitFieldOperation.Get, bitFieldType, Some(offset))
    def incrBy(bitFieldType: BitFieldDataType, value: Long): BitFieldCommand = BitFieldCommand(BitFieldOperation.Incrby, bitFieldType, None, Some(value))
    def incrBy(bitFieldType: BitFieldDataType, value: Long, offset: Int): BitFieldCommand = BitFieldCommand(BitFieldOperation.Incrby, bitFieldType, Some(offset), Some(value))
    def overflowWrap(bitFieldType: BitFieldDataType): BitFieldCommand = BitFieldCommand(BitFieldOperation.OverflowWrap, bitFieldType)
    def overflowSat(bitFieldType: BitFieldDataType): BitFieldCommand = BitFieldCommand(BitFieldOperation.OverflowSat, bitFieldType)
    def overflowFail(bitFieldType: BitFieldDataType): BitFieldCommand = BitFieldCommand(BitFieldOperation.OverflowFail, bitFieldType)
  }
}
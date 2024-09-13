package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import java.time.Instant

/**
 * Asynchronous commands for Redis streams
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisStreamAsyncCommands [K, V]{
  /**
   * Acknowledge one or more messages as processed
   * @param key The stream key
   * @param group The consumer group
   * @param ids The message IDs to acknowledge
   * @return The number of acknowledged messages
   */
  def xAck(key: K, group: K, ids: String*): Future[Long]

  /**
   * Add a new entry to a stream
   * @param key The stream key
   * @param values The message field-value pairs
   * @return The message ID
   */
  def xAdd(key: K, values: Map[K, V]): Future[String]

  /**
   * Add a new entry to a stream
   * @param key The stream key
   * @param id Specify the message ID instead of generating one automatically
   * @param values The message field-value pairs
   * @return the message ID
   */
  def xAdd(key: K, id: K, values: Map[K, V]): Future[String]

  /**
   * Claim messages from a stream
   * @param key The stream key
   * @param group The consumer group
   * @param consumer The consumer name
   * @param minIdleTime The minimum idle time
   * @param startId The start ID
   * @param justId Whether to return just the IDs
   * @param count The number of messages to claim
   * @return The claimed messages
   */
  def xAutoClaim(
    key: K, group: K, consumer: K,
    minIdleTime: FiniteDuration,
    startId: String,
    justId: Boolean=false,
    count: Option[Long] = None
  ): Future[RedisStreamAsyncCommands.ClaimedMessages[K, V]]

  /**
   * Gets ownership of one or multiple messages in the Pending Entries List of a given stream consumer group.
   * @param key The stream key
   * @param group The consumer group
   * @param consumer The consumer name
   * @param minIdleTime The minimum idle time
   * @param messageIds The message IDs to claim
   * @param justId Whether to return just the IDs.
   *               Set the JUSTID flag to return just the message id and do not increment the retry counter.
   *               The message body is not returned when calling XCLAIM.
   * @param force Creates the pending message entry in the PEL even if certain specified IDs are not already in the PEL assigned to a different client.
   *              However the message must be exist in the stream, otherwise the IDs of non existing messages are ignored.
   * @param retryCount Set the retry counter to the specified value. This counter is incremented every time a message is delivered again.
   *                   Normally XCLAIM does not alter this counter, which is just served to clients when the XPENDING command is called:
   *                   this way clients can detect anomalies, like messages that are never processed for some reason after a big number of delivery attempts.
   * @param idle Set the idle time (last time it was delivered) of the message.
   *             If IDLE is not specified, an IDLE of 0 is assumed, that is, the time count is reset
   *             because the message has now a new owner trying to process it.
   * @param time This is the same as IDLE but instead of a relative amount of milliseconds,
   *             it sets the idle time to a specific unix time (in milliseconds).
   *             This is useful in order to rewrite the AOF file generating XCLAIM commands.
   * @return The claimed messages
   */
  def xClaim(
    key: K,
    group: K,
    consumer: K,
    minIdleTime: FiniteDuration,
    messageIds: List[String],
    justId: Boolean = false,
    force: Boolean = false,
    retryCount: Option[Long] = None,
    idle: Option[FiniteDuration] = None,
    time: Option[Instant] = None,
  ): Future[List[RedisStreamAsyncCommands.StreamMessage[K, V]]]


  /**
   * Delete one or more messages from a stream
   * @param key The stream key
   * @param ids The message IDs to delete
   * @return The number of deleted messages
   */
  def xDel(key: K, ids: String*): Future[Long]

  /**
   * Create a new consumer group
   * @param streamOffset The stream offset consisting of the stream key and the offset
   * @param group The group name
   * @param mkStream Whether to create the stream if it does not exist
   * @param entriesRead The number of entries to read
   * @return Unit
   */
  def xGroupCreate(
    streamOffset: RedisStreamAsyncCommands.StreamOffset[K],
    group: K,
    mkStream: Boolean = false,
    entriesRead: Option[Long] = None
  ): Future[Unit]


  /**
   * Create a new consumer group
   * @param key The stream key
   * @param group The group name
   * @param consumer The consumer name
   * @return Whether the consumer was created
   */
  def xGroupCreateConsumer(key: K, group: K, consumer: K): Future[Boolean]

  /**
   * Delete a consumer from a consumer group
   * @param key The stream key
   * @param group The group name
   * @param consumer The consumer name
   * @return The number of pending messages the consumer had before it was deleted.
   */
  def xGroupDelConsumer(key: K, group: K, consumer: K): Future[Long]

  /**
   * Destroy a consumer group
   * @param key The stream key
   * @param group The group name
   * @return Whether the group was destroyed
   */
  def xGroupDestroy(key: K, group: K): Future[Boolean]

  /**
   * Set the ID of the last message successfully processed by a consumer
   * @param streamOffset The stream offset consisting of the stream key and the offset
   * @param group The group name
   * @return Unit
   */
  def xGroupSetId(streamOffset: RedisStreamAsyncCommands.StreamOffset[K], group: K): Future[Unit]

  /**
   * Get the length of a stream
   * @param key The stream key
   * @return The length of the stream
   */
  def xLen(key: K): Future[Long]

  /**
   * Get the pending messages for a consumer group
   * @param key The stream key
   * @param group The group name
   * @return The pending messages
   */
  def xPending(key: K, group: K): Future[RedisStreamAsyncCommands.PendingMessages]

  /**
   * Get the pending messages for a consumer group
   * @param key The stream key
   * @param group The group name
   * @param consumer The consumer name
   * @return The pending messages
   */
  def xPending(
    key: K,
    group: K,
    consumer: K,
    range: Option[RedisStreamAsyncCommands.XRange] = None,
    limit: Option[RedisStreamAsyncCommands.XRangeLimit] = None,
    idle: Option[FiniteDuration] = None
  ): Future[List[RedisStreamAsyncCommands.PendingMessage]]

  /**
   * Get a range of messages from a stream
   * @param key The stream key
   * @param range The range to get
   * @return The messages in the range
   */
  def xRange(key: K, range: RedisStreamAsyncCommands.XRange): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]]

  /**
   * Get a range of messages from a stream with a limit
   * @param key The stream key
   * @param range The range to get
   * @param limit The limit
   * @return The messages in the range
   */
  def xRange(key: K, range: RedisStreamAsyncCommands.XRange, limit: RedisStreamAsyncCommands.XRangeLimit): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]]

  /**
   * Read messages from one or more streams
   * @param streams The streams to read from
   * @return The messages read
   */
  def xRead(streams: RedisStreamAsyncCommands.StreamOffset[K]*): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]]

  /**
   * Read messages from one or more streams
   * @param streams The streams to read from
   * @param count The maximum number of messages to read
   * @param block The block duration
   * @return The messages read
   */
  def xRead(streams: List[RedisStreamAsyncCommands.StreamOffset[K]], count: Option[Long] = None, block: Option[FiniteDuration] = None, noAck: Boolean = false): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]]

  /**
   * Read messages from a consumer group
   * @param group The group name
   * @param consumer The consumer name
   * @param streams The streams to read from
   * @return The messages read
   */
  def xReadGroup(
    group: K,
    consumer: K,
    streams: RedisStreamAsyncCommands.StreamOffset[K]*,
  ): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]]

  /**
   * Read messages from a consumer group
   * @param group The group name
   * @param consumer The consumer name
   * @param streams The streams to read from
   * @param count The maximum number of messages to read
   * @param block The block duration
   * @param noAck Whether to acknowledge the messages
   * @return The messages read
   */
  def xReadGroup(
    group: K,
    consumer: K,
    streams: List[RedisStreamAsyncCommands.StreamOffset[K]],
    count: Option[Long] = None,
    block: Option[FiniteDuration] = None,
    noAck: Boolean = false
  ): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]]

  /**
   * Get a reverse range of messages from a stream
   * @param key The stream key
   * @param range The range to get
   * @return The messages in the range
   */
  def xRevRange(key: K, range: RedisStreamAsyncCommands.XRange): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]]

  /**
   * Trim the stream to a certain size
   * @param key The stream key
   * @param count The number of messages to keep
   * @return The number of messages removed
   */
  def xTrim(key: K, count: Long): Future[Long]

  /**
   * Trim the stream to a certain size
   * @param key The stream key
   * @param approximateTrimming Whether to use approximate trimming
   * @param count The number of messages to keep
   * @return The number of messages removed
   */
  def xTrim(key: K, approximateTrimming: Boolean, count: Long): Future[Long]

  /**
   * Trim the stream to a certain size
   * @param key The stream key
   * @param args The trimming arguments
   * @return The number of messages removed
   */
  def xTrim(key: K, args: RedisStreamAsyncCommands.XTrimArgs): Future[Long]


}

object RedisStreamAsyncCommands {
  /**
   * A message in a stream
   * @param stream The stream key
   * @param id The message ID
   * @param entries The message field-value pairs
   * @tparam K The key type
   * @tparam V The value type
   */
  case class StreamMessage[K, V](stream: K, id: String, entries: Map[K, V] = Map.empty)

  /**
   * A group of claimed messages
   * @param id The ID of the group
   * @param messages The claimed messages
   * @tparam K The key type
   * @tparam V The value type
   */
  case class ClaimedMessages[K, V](id: String, messages: List[StreamMessage[K, V]])


  /**
   * A range of values
   *
   * @param lower The lower bound
   * @param upper The upper bound
   */
  final case class XRange(lower: XRange.Boundary, upper: XRange.Boundary)

  object XRange {

    /**
     * A boundary value, used for range queries (upper or lower bounds)
     * @param value An optional value to use as the boundary. If None, it is unbounded.
     * @param inclusive Whether the boundary is inclusive. default is false.
     */
    case class Boundary(value: Option[String] = None, inclusive: Boolean = false)

    object Boundary {
      /**
       * Create a new inclusive boundary
       * @param value The value
       * @return The boundary
       */
      def including(value: String): Boundary = Boundary(Some(value), inclusive = true)

      /**
       * Create a new inclusive boundary
       * @param value The value
       * @return The boundary
       */
      def including(value: java.time.Instant): Boundary = Boundary(Some(s"${value.toEpochMilli.toString}-0"), inclusive = true)

      /**
       * Create a new exclusive boundary
       * @param value The value
       * @return The boundary
       */
      def excluding(value: String): Boundary = Boundary(Some(value))

      /**
       * Create a new exclusive boundary
       * @param value The value
       * @return The boundary
       */
      def excluding(value: java.time.Instant): Boundary = Boundary(Some(s"${value.toEpochMilli.toString}-0"))

      /**
       * Create an unbounded boundary
       * @return The boundary
       */
      def unbounded: Boundary = Boundary(None)
    }

    /**
     * Create a new range
     * @param lower The lower bound
     * @param upper The upper bound
     * @return The range
     */
    def apply(lower: String, upper: String): XRange = XRange(Boundary.including(lower), Boundary.including(upper))


    /**
     * Create a new range
     * @param lower The lower bound
     * @param upper The upper bound
     * @return The range
     */
    def apply(lower: java.time.Instant, upper: java.time.Instant): XRange = XRange(Boundary.including(lower), Boundary.including(upper))

    /**
     * Create a new range
     * @param lower The lower bound
     * @param upper The upper bound
     * @return The range
     */
    def including(lower: String, upper: String): XRange = XRange(Boundary.including(lower), Boundary.including(upper))

    /**
     * Create a new range
     * @param lower The lower bound
     * @param upper The upper bound
     * @return The range
     */
    def including(lower: java.time.Instant, upper: java.time.Instant): XRange = XRange(Boundary.including(lower), Boundary.including(upper))

    /**
     * Create a new range
     * @param lower The lower bound
     * @param upper The upper bound
     * @return The range
     */
    def from(lower: String, upper: String, inclusive: Boolean = false): XRange = {
      if(inclusive) XRange(Boundary.including(lower), Boundary.including(upper))
      else XRange(Boundary.excluding(lower), Boundary.excluding(upper))
    }

    def from(lower: java.time.Instant, upper: java.time.Instant): XRange = {
      XRange(Boundary.excluding(lower), Boundary.excluding(upper))
    }

    def from(lower: java.time.Instant, upper: java.time.Instant, inclusive: Boolean): XRange = {
      if(inclusive) XRange(Boundary.including(lower), Boundary.including(upper))
      else XRange(Boundary.excluding(lower), Boundary.excluding(upper))
    }


    /**
     * Create a new range from lower to unbounded
     * @param lower The start boundary
     * @return The range
     */
    def fromLower(lower: String, inclusive: Boolean = false): XRange = {
      if(inclusive) XRange(Boundary.including(lower), Boundary.unbounded)
      else XRange(Boundary.excluding(lower), Boundary.unbounded)
    }

    def fromLower(lower: java.time.Instant): XRange = {
      XRange(Boundary.excluding(lower), Boundary.unbounded)
    }

    def fromLower(lower: java.time.Instant, inclusive: Boolean): XRange = {
      if(inclusive) XRange(Boundary.including(lower), Boundary.unbounded)
      else XRange(Boundary.excluding(lower), Boundary.unbounded)
    }

    /**
     * Create a new range to upper bound
     * @param upper The upper boundary
     * @return The range
     */
    def toUpper(upper: String, inclusive: Boolean = false): XRange = {
      if(inclusive) XRange(Boundary.unbounded, Boundary.including(upper))
      else XRange(Boundary.unbounded, Boundary.excluding(upper))
    }

    def toUpper(upper: java.time.Instant): XRange = {
      XRange(Boundary.unbounded, Boundary.excluding(upper))
    }

    def toUpper(upper: java.time.Instant, inclusive: Boolean): XRange = {
      if(inclusive) XRange(Boundary.unbounded, Boundary.including(upper))
      else XRange(Boundary.unbounded, Boundary.excluding(upper))
    }

    /**
     * Create a new unbounded range
     * @return The range
     */
    def unbounded: XRange = new XRange(Boundary.unbounded, Boundary.unbounded)
  }

  /**
   * A range limit
   *
   * @param offset The offset
   * @param count The count
   */
  final case class XRangeLimit(offset: Long, count: Long)


  /**
   * Arguments for trimming a stream
   * @param maxLen The maximum length of the stream
   * @param approximateTrimming Whether to use approximate trimming
   * @param exactTrimming Whether to use exact trimming
   * @param minId The minimum ID to trim to
   * @param limit The maximum number of elements to trim
   */
  case class XTrimArgs(
    maxLen: Option[Long] = None,
    approximateTrimming: Boolean = false,
    exactTrimming: Boolean = false,
    minId: Option[String] = None,
    limit: Option[Long] = None
  )

  case class StreamOffset[K](name: K, offset: String)

  object StreamOffset {
    def latest[K](key: K): StreamOffset[K] = StreamOffset(key, "$")

    def earliest[K](key: K): StreamOffset[K] = StreamOffset(key, "0")

    def lastConsumed[K](key: K): StreamOffset[K] = StreamOffset(key, ">")
  }

  /**
   * A pending message
   * @param count The number of pending messages
   * @param messageIds a range of message IDs
   * @param consumerMessageCount The number of messages per consumer
   */
  case class PendingMessages(count: Long, messageIds: XRange, consumerMessageCount: Map[String, Long] = Map.empty)


  /**
   * A pending message
   * @param id The message ID
   * @param consumer The consumer name
   * @param sinceLastDelivery The time since the last delivery
   * @param reDeliveryCount The number of times the message has been redelivered
   */
  case class PendingMessage(id: String, consumer: String, sinceLastDelivery: FiniteDuration, reDeliveryCount: Long)

}

// need to implement:
//xinfoStream
//xinfoGroups
//xinfoConsumers
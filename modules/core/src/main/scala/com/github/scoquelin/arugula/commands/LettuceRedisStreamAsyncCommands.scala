package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, MapHasAsScala}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

import com.github.scoquelin.arugula.commands.RedisStreamAsyncCommands.XRange
import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

import java.time.Instant

trait LettuceRedisStreamAsyncCommands[K, V] extends RedisStreamAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {
  override def xAck(key: K, group: K, ids: String*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.xack(key, group, ids: _*)).map(Long2long)

  override def xAdd(key: K, values: Map[K, V]): Future[String] = {
    delegateRedisClusterCommandAndLift(_.xadd(key, values.asJava))
  }

  override def xAdd(key: K, id: K, values: Map[K, V]): Future[String] ={
    delegateRedisClusterCommandAndLift(_.xadd(key, id, values.asJava))
  }

  override def xAutoClaim(key: K, group: K, consumer: K, minIdleTime: FiniteDuration, startId: String, justId: Boolean=false, count: Option[Long] = None): Future[RedisStreamAsyncCommands.ClaimedMessages[K, V]] = {
    val args: io.lettuce.core.XAutoClaimArgs[K] = io.lettuce.core.XAutoClaimArgs.Builder.xautoclaim(
      io.lettuce.core.Consumer.from(group, consumer),
      minIdleTime.toJava,
      startId
    )
    if(justId) args.justid()
    count.foreach(args.count)

    delegateRedisClusterCommandAndLift(_.xautoclaim(key, args)).map{ claimedMessages =>
      RedisStreamAsyncCommands.ClaimedMessages(claimedMessages.getId, claimedMessages.getMessages.asScala.map { kv =>
        RedisStreamAsyncCommands.StreamMessage(key, kv.getId, kv.getBody.asScala.toMap)
      }.toList)
    }
  }

  override def xClaim(key: K,
    group: K,
    consumer: K,
    minIdleTime: FiniteDuration,
    messageIds: List[String],
    justId: Boolean = false,
    force: Boolean = false,
    retryCount: Option[Long] = None,
    idle: Option[FiniteDuration] = None,
    time: Option[Instant] = None,
  ): Future[List[RedisStreamAsyncCommands.StreamMessage[K, V]]] = {
    val args: io.lettuce.core.XClaimArgs = io.lettuce.core.XClaimArgs.Builder.minIdleTime(minIdleTime.toJava)
    if (justId) args.justid()
    if(force) args.force()
    retryCount.foreach(args.retryCount)
    idle.foreach(d => args.idle(d.toJava))
    time.foreach(d => args.time(d.toEpochMilli))
    delegateRedisClusterCommandAndLift(_.xclaim(key, io.lettuce.core.Consumer.from(
      group, consumer
    ), args, messageIds: _*)).map(_.asScala.toList.map { kv =>
      RedisStreamAsyncCommands.StreamMessage(key, kv.getId, kv.getBody.asScala.toMap)
    })
  }

  override def xDel(key: K, ids: String*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.xdel(key, ids: _*)).map(Long2long)

  override def xGroupCreate(
    streamOffset: RedisStreamAsyncCommands.StreamOffset[K],
    group: K,
    mkStream: Boolean = false,
    entriesRead: Option[Long] = None
  ): Future[Unit] = {
    val args = io.lettuce.core.XGroupCreateArgs.Builder.mkstream(mkStream)
    entriesRead.foreach(args.entriesRead)
    delegateRedisClusterCommandAndLift(_.xgroupCreate(LettuceRedisStreamAsyncCommands.streamOffsetToJava(streamOffset), group, args)).map(_ => ())
  }

  override def xGroupCreateConsumer(key: K, group: K, consumer: K): Future[Boolean] = {
    delegateRedisClusterCommandAndLift(_.xgroupCreateconsumer(key, io.lettuce.core.Consumer.from(group, consumer))).map(Boolean2boolean)
  }

  override def xGroupDelConsumer(key: K, group: K, consumer: K): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.xgroupDelconsumer(key, io.lettuce.core.Consumer.from(group, consumer))).map(Long2long)
  }

  override def xGroupDestroy(key: K, group: K): Future[Boolean] = {
    delegateRedisClusterCommandAndLift(_.xgroupDestroy(key, group)).map(Boolean2boolean)
  }

  override def xGroupSetId(
    streamOffset: RedisStreamAsyncCommands.StreamOffset[K],
    group: K): Future[Unit] = {
    delegateRedisClusterCommandAndLift(_.xgroupSetid(LettuceRedisStreamAsyncCommands.streamOffsetToJava(streamOffset), group)).map(_ => ())
  }

  override def xLen(key: K): Future[Long] =
    delegateRedisClusterCommandAndLift(_.xlen(key)).map(Long2long)

  override def xPending(
    key: K,
    group: K,
  ): Future[RedisStreamAsyncCommands.PendingMessages] = {
    delegateRedisClusterCommandAndLift(_.xpending(key, group)).map(LettuceRedisStreamAsyncCommands.pendingMessagesFromJava)
  }


  override def xPending(
    key: K,
    group: K,
    consumer: K,
    range: Option[RedisStreamAsyncCommands.XRange] = None,
    limit: Option[RedisStreamAsyncCommands.XRangeLimit] = None,
    idle: Option[FiniteDuration] = None
  ): Future[List[RedisStreamAsyncCommands.PendingMessage]] = {
    val args = new io.lettuce.core.XPendingArgs[K]()
    args.consumer(io.lettuce.core.Consumer.from(group, consumer))
    range.foreach(r => args.range(LettuceRedisStreamAsyncCommands.toJavaRange(r)))
    limit.foreach(l => args.limit(io.lettuce.core.Limit.create(l.offset, l.count)))
    idle.foreach(d => args.idle(d.toJava))
    delegateRedisClusterCommandAndLift(_.xpending(key, args)).map(_.asScala.toList.map{
      pendingMessages => LettuceRedisStreamAsyncCommands.pendingMessageFromJava(pendingMessages)
    })
  }


  override def xRange(key: K, range: RedisStreamAsyncCommands.XRange): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]] =
    delegateRedisClusterCommandAndLift(_.xrange(key, LettuceRedisStreamAsyncCommands.toJavaRange(range))).map(_.asScala.map { kv =>
      RedisStreamAsyncCommands.StreamMessage(key, kv.getId, kv.getBody.asScala.toMap)
    }.toSeq)

  override def xRange(key: K,
    range: XRange,
    limit: RedisStreamAsyncCommands.XRangeLimit): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]] = {
    val args = LettuceRedisStreamAsyncCommands.toJavaRange(range)
    val limitArgs = io.lettuce.core.Limit.create(limit.offset, limit.count)
    delegateRedisClusterCommandAndLift(_.xrange(key, args, limitArgs)).map(_.asScala.map { kv =>
      RedisStreamAsyncCommands.StreamMessage(kv.getStream, kv.getId, kv.getBody.asScala.toMap)
    }.toSeq)
  }

  override def xRead(streams: RedisStreamAsyncCommands.StreamOffset[K]*): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]] = {
    delegateRedisClusterCommandAndLift(_.xread(streams.map(LettuceRedisStreamAsyncCommands.streamOffsetToJava).toArray:_*)).map(_.asScala.map { kv =>
      RedisStreamAsyncCommands.StreamMessage(kv.getStream, kv.getId, kv.getBody.asScala.toMap)
    }.toSeq)
  }

  override def xRead(
    streams: List[RedisStreamAsyncCommands.StreamOffset[K]],
    count: Option[Long],
    block: Option[FiniteDuration],
    noAck: Boolean = false
  ): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]] = {
    (count, block,  noAck) match {
      case (None, None, false) => xRead(streams: _*)
      case _ => val args = new io.lettuce.core.XReadArgs()
        count.foreach(args.count)
        block.foreach(d => args.block(d.toJava))
        if(noAck) args.noack(java.lang.Boolean.TRUE)
        delegateRedisClusterCommandAndLift(_.xread(args, streams.map(LettuceRedisStreamAsyncCommands.streamOffsetToJava):_*)).map(_.asScala.map { kv =>
          RedisStreamAsyncCommands.StreamMessage(kv.getStream, kv.getId, kv.getBody.asScala.toMap)
        }.toSeq)
    }
  }

  override def xReadGroup(group: K, consumer: K, streams: RedisStreamAsyncCommands.StreamOffset[K]*): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]] = {
    delegateRedisClusterCommandAndLift(_.xreadgroup(io.lettuce.core.Consumer.from(group, consumer), streams.map(LettuceRedisStreamAsyncCommands.streamOffsetToJava):_*)).map(_.asScala.map { kv =>
      RedisStreamAsyncCommands.StreamMessage(kv.getStream, kv.getId, kv.getBody.asScala.toMap)
    }.toSeq)
  }

  override def xReadGroup(group: K,
    consumer: K,
    streams: List[RedisStreamAsyncCommands.StreamOffset[K]],
    count: Option[Long],
    block: Option[FiniteDuration],
    noAck: Boolean): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]] = {
    (count, block, noAck) match {
      case (None, None, false) => xReadGroup(group, consumer, streams: _*)
      case _ =>
        val args = new io.lettuce.core.XReadArgs()
        count.foreach(args.count)
        block.foreach(d => args.block(d.toJava))
        if(noAck) args.noack(java.lang.Boolean.TRUE)
        delegateRedisClusterCommandAndLift(_.xreadgroup(io.lettuce.core.Consumer.from(group, consumer), args, streams.map(LettuceRedisStreamAsyncCommands.streamOffsetToJava):_*)).map(_.asScala.map { kv =>
        RedisStreamAsyncCommands.StreamMessage(kv.getStream, kv.getId, kv.getBody.asScala.toMap)
      }.toSeq)
    }
  }

  override def xRevRange(key: K, range: XRange): Future[Seq[RedisStreamAsyncCommands.StreamMessage[K, V]]] = {
    delegateRedisClusterCommandAndLift(_.xrevrange(key, LettuceRedisStreamAsyncCommands.toJavaRange(range))).map(_.asScala.map { kv =>
      RedisStreamAsyncCommands.StreamMessage(key, kv.getId, kv.getBody.asScala.toMap)
    }.toSeq)
  }

  override def xTrim(key: K, count: Long): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.xtrim(key, count)).map(Long2long)
  }

  override def xTrim(key: K, approximateTrimming: Boolean, count: Long): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.xtrim(key, approximateTrimming, count)).map(Long2long)
  }

  override def xTrim(key: K, args: RedisStreamAsyncCommands.XTrimArgs): Future[Long] = {
    val trimArgs = new io.lettuce.core.XTrimArgs()
    if(args.approximateTrimming) trimArgs.approximateTrimming()
    if(args.exactTrimming) trimArgs.exactTrimming()
    args.maxLen.foreach(trimArgs.maxlen)
    args.limit.foreach(trimArgs.limit)
    args.minId.foreach(trimArgs.minId)
    delegateRedisClusterCommandAndLift(_.xtrim(key, trimArgs)).map(Long2long)
  }

}

object LettuceRedisStreamAsyncCommands {

  private [arugula] def toJavaRange(range: XRange): io.lettuce.core.Range[String] = {
    io.lettuce.core.Range.from(toJavaBoundary(range.lower), toJavaBoundary(range.upper))
  }

  private [commands] def toJavaBoundary(boundary: XRange.Boundary): io.lettuce.core.Range.Boundary[String] = {
    boundary.value match {
      case Some(value) if boundary.inclusive => io.lettuce.core.Range.Boundary.including(value)
      case Some(value) => io.lettuce.core.Range.Boundary.excluding(value)
      case None => io.lettuce.core.Range.Boundary.unbounded()
    }
  }

  private[commands] def streamOffsetToJava[K](streamOffset: RedisStreamAsyncCommands.StreamOffset[K]): io.lettuce.core.XReadArgs.StreamOffset[K] = {
    io.lettuce.core.XReadArgs.StreamOffset.from[K](streamOffset.name, streamOffset.offset)
  }

  def xRangeFromJava(range: io.lettuce.core.Range[String]): RedisStreamAsyncCommands.XRange = {
    RedisStreamAsyncCommands.XRange(
      boundaryFromJava(range.getLower),
      boundaryFromJava(range.getUpper)
    )
  }

  def boundaryFromJava(boundary: io.lettuce.core.Range.Boundary[String]): RedisStreamAsyncCommands.XRange.Boundary = {
    boundary match {
      case b if b.isUnbounded => RedisStreamAsyncCommands.XRange.Boundary.unbounded
      case b if b.isIncluding => RedisStreamAsyncCommands.XRange.Boundary.including(b.getValue)
      case b => RedisStreamAsyncCommands.XRange.Boundary.excluding(b.getValue)
    }
  }

  def pendingMessagesFromJava(pendingMessages: io.lettuce.core.models.stream.PendingMessages): RedisStreamAsyncCommands.PendingMessages = {
    RedisStreamAsyncCommands.PendingMessages(
      pendingMessages.getCount,
      xRangeFromJava(pendingMessages.getMessageIds),
      pendingMessages.getConsumerMessageCount.asScala.map{
        case (consumer, count) => consumer -> Long2long(count)
      }.toMap
    )
  }

  def pendingMessageFromJava(pendingMessage: io.lettuce.core.models.stream.PendingMessage): RedisStreamAsyncCommands.PendingMessage = {
    RedisStreamAsyncCommands.PendingMessage(
      pendingMessage.getId,
      pendingMessage.getConsumer,
      pendingMessage.getSinceLastDelivery.toScala,
      pendingMessage.getRedeliveryCount
    )
  }
}

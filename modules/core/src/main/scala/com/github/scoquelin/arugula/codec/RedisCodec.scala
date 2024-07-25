package com.github.scoquelin.arugula.codec

import io.lettuce.core.codec.{ToByteBufEncoder, RedisCodec => JRedisCodec, StringCodec => JStringCodec}

private[arugula] case class RedisCodec[K, V](underlying: JRedisCodec[K, V])

object RedisCodec {
  val Utf8WithValueAsStringCodec: RedisCodec[String, String] = RedisCodec(JStringCodec.UTF8)
  val Utf8WithValueAsLongCodec: RedisCodec[String, Long] = RedisCodec(LongCodec)
}

private object LongCodec extends JRedisCodec[String, Long] with ToByteBufEncoder[String, Long] {

  import java.nio.ByteBuffer
  import io.netty.buffer.ByteBuf

  private val codec = JStringCodec.UTF8

  override def decodeKey(bytes: ByteBuffer): String = codec.decodeKey(bytes)

  override def encodeKey(key: String): ByteBuffer = codec.encodeKey(key)

  override def encodeValue(value: Long): ByteBuffer = codec.encodeValue(value.toString)

  override def decodeValue(bytes: ByteBuffer): Long = codec.decodeValue(bytes).toLong

  override def encodeKey(key: String, target: ByteBuf): Unit = codec.encodeKey(key, target)

  override def encodeValue(value: Long, target: ByteBuf): Unit = codec.encodeValue(value.toString, target)

  override def estimateSize(keyOrValue: scala.Any): Int = codec.estimateSize(keyOrValue)
}

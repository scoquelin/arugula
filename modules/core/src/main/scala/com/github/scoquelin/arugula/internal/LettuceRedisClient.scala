package com.github.scoquelin.arugula.internal

import com.github.scoquelin.arugula.codec.RedisCodec
import com.github.scoquelin.arugula.connection.RedisConnection

private[arugula] trait LettuceRedisClient {
  def getRedisConnection[K, V](codec: RedisCodec[K, V]): RedisConnection[K, V]
  def close: Unit
}

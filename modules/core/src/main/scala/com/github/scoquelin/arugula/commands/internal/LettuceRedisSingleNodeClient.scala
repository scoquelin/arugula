package com.github.scoquelin.arugula.commands.internal

import com.github.scoquelin.arugula.codec.RedisCodec
import com.github.scoquelin.arugula.config.LettuceRedisClientConfig
import com.github.scoquelin.arugula.connection.{RedisConnection, RedisStatefulConnection}

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._

import io.lettuce.core.{RedisURI, RedisClient => JRedisClient}

private class LettuceRedisSingleNodeClient(underlying: JRedisClient, redisURI: RedisURI)(implicit ec: ExecutionContext) extends LettuceRedisClient {
  override def getRedisConnection[K, V](codec: RedisCodec[K, V]): RedisConnection[K, V] =
    new RedisStatefulConnection[K, V](underlying.connectAsync(codec.underlying, redisURI).toCompletableFuture.asScala)

  override def close: Unit = underlying.close()
}

private[commands] object LettuceRedisSingleNodeClient {
  def apply(redisClientConfig: LettuceRedisClientConfig)(implicit ec: ExecutionContext): LettuceRedisClient =
    new LettuceRedisSingleNodeClient(
      JRedisClient.create(redisClientConfig.clientResources),
      RedisURI.builder()
        .withHost(redisClientConfig.host)
        .withPort(redisClientConfig.port)
        .build()
    )
}

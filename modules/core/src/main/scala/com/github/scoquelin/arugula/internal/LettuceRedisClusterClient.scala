package com.github.scoquelin.arugula.internal

import com.github.scoquelin.arugula.codec.RedisCodec
import com.github.scoquelin.arugula.config.LettuceRedisClientConfig
import com.github.scoquelin.arugula.connection.{RedisClusterStatefulConnection, RedisConnection}

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._

import io.lettuce.core.RedisURI
import io.lettuce.core.cluster.{RedisClusterClient => JRedisClusterClient}

private class LettuceRedisClusterClient(underlying: JRedisClusterClient)(implicit ec: ExecutionContext) extends LettuceRedisClient {
  override def getRedisConnection[K, V](codec: RedisCodec[K, V]): RedisConnection[K, V] =
    new RedisClusterStatefulConnection(underlying.connectAsync(codec.underlying).toCompletableFuture.asScala)

  override def close: Unit = underlying.close()
}

private[arugula] object LettuceRedisClusterClient {
  def apply(redisClientConfig: LettuceRedisClientConfig)(implicit ec: ExecutionContext): LettuceRedisClient = {
    val client = JRedisClusterClient.create(
      redisClientConfig.clientResources,
      RedisURI.builder()
        .withHost(redisClientConfig.host)
        .withPort(redisClientConfig.port)
        .build()
    )
    client.getPartitions

    new LettuceRedisClusterClient(
      client
    )
  }
}

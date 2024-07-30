package com.github.scoquelin.arugula

import com.github.scoquelin.arugula.codec.{LongCodec, RedisCodec}
import com.github.scoquelin.arugula.config.LettuceRedisClientConfig
import io.lettuce.core.codec.{StringCodec => JStringCodec}

import scala.concurrent.ExecutionContext

sealed trait RedisConnectionType

case object SingleNode extends RedisConnectionType

case object Cluster extends RedisConnectionType

trait CachedClients {
  def getClient[K, V](codec: RedisCodec[K, V], connectionType: RedisConnectionType): RedisCommandsClient[K, V]
}

private class RedisCommandsCachedClients(redisSingleNodeClientWithStringValue: RedisCommandsClient[String, String],
                                         redisSingleNodeClientWithLongValue: RedisCommandsClient[String, Long],
                                         redisClusterClientWithStringValue: RedisCommandsClient[String, String],
                                         redisClusterClientWithLongValue: RedisCommandsClient[String, Long]) extends CachedClients {

  override def getClient[K, V](codec: RedisCodec[K, V], connectionType: RedisConnectionType): RedisCommandsClient[K, V] = {
    (codec, connectionType) match {
      case (RedisCodec(JStringCodec.UTF8), SingleNode) => redisSingleNodeClientWithStringValue.asInstanceOf[RedisCommandsClient[K, V]]
      case (RedisCodec(JStringCodec.UTF8), Cluster) => redisClusterClientWithStringValue.asInstanceOf[RedisCommandsClient[K, V]]
      case (RedisCodec(LongCodec), SingleNode) => redisSingleNodeClientWithLongValue.asInstanceOf[RedisCommandsClient[K, V]]
      case (RedisCodec(LongCodec), Cluster) => redisClusterClientWithLongValue.asInstanceOf[RedisCommandsClient[K, V]]
      case (codec, connectionType) => throw new IllegalStateException(s"Codec $codec not supported for connection type $connectionType")
    }
  }
}

object RedisCommandsCachedClients {
  def apply(singleNodeConfig: LettuceRedisClientConfig, clusterConfig: LettuceRedisClientConfig)(implicit ec: ExecutionContext): CachedClients = {
    val redisSingleNodeClientWithStringValue = LettuceRedisCommandsClient(singleNodeConfig, RedisCodec.Utf8WithValueAsStringCodec)
    val redisSingleNodeClientWithLongValue = LettuceRedisCommandsClient(singleNodeConfig, RedisCodec.Utf8WithValueAsLongCodec)
    val redisClusterClientWithStringValue = LettuceRedisCommandsClient(clusterConfig, RedisCodec.Utf8WithValueAsStringCodec)
    val redisClusterClientWithLongValue = LettuceRedisCommandsClient(clusterConfig, RedisCodec.Utf8WithValueAsLongCodec)
    new RedisCommandsCachedClients(
      redisSingleNodeClientWithStringValue,
      redisSingleNodeClientWithLongValue,
      redisClusterClientWithStringValue,
      redisClusterClientWithLongValue
    )
  }
}

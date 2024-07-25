package com.github.scoquelin.arugula.commands.client

import com.github.scoquelin.arugula.api.commands.RedisAsyncCommands
import com.github.scoquelin.arugula.api.commands.client.RedisCommandsClient
import com.github.scoquelin.arugula.codec.RedisCodec
import com.github.scoquelin.arugula.commands.LettuceRedisAsyncCommands
import com.github.scoquelin.arugula.commands.internal.{LettuceRedisClusterClient, LettuceRedisSingleNodeClient}
import com.github.scoquelin.arugula.config.LettuceRedisClientConfig
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[client] class LettuceRedisCommandsClient[K, V](redisAsyncCommands: RedisAsyncCommands[K, V]) extends RedisCommandsClient[K, V] {
  def sendCommand[T](command: RedisAsyncCommands[K, V] => Future[T]): Future[T] = command(redisAsyncCommands)
}

object LettuceRedisCommandsClient {
  val logger = LoggerFactory.getLogger(this.getClass)

  def apply(redisClientConfig: LettuceRedisClientConfig)(implicit ec: ExecutionContext): RedisCommandsClient[String, String] =
    apply(redisClientConfig, RedisCodec.Utf8WithValueAsStringCodec)

  def apply[K, V](redisClientConfig: LettuceRedisClientConfig, redisCodec: RedisCodec[K, V])(implicit ec: ExecutionContext): RedisCommandsClient[K, V] = {
    //Check if target Redis server is "cluster-enabled" by contacting Redis server using INFO and instantiate the appropriate client (single-node or cluster)
    checkIfClusterEnabled(redisClientConfig) match {
      case Success(clusterEnabled@true) =>
        logger.info(s"Instantiating new RedisCommandsClient with cluster mode = $clusterEnabled")
        new LettuceRedisCommandsClient(
          new LettuceRedisAsyncCommands(
            LettuceRedisClusterClient.apply(redisClientConfig).getRedisConnection(redisCodec),
            cluster = clusterEnabled
          )
        )
      case Success(clusterDisabled@false) =>
        logger.info(s"Instantiating new RedisCommandsClient with cluster mode = $clusterDisabled")
        new LettuceRedisCommandsClient(
          new LettuceRedisAsyncCommands(
            LettuceRedisSingleNodeClient.apply(redisClientConfig).getRedisConnection(redisCodec),
            cluster = clusterDisabled
          )
        )
      case Failure(exception) =>
        throw new IllegalStateException("Unable to instantiate Redis bootstrap client", exception)
    }
  }

  private def checkIfClusterEnabled(redisClientConfig: LettuceRedisClientConfig): Try[Boolean] = {
    import io.lettuce.core.{RedisURI, RedisClient => JRedisClient}

    def parseInfo(info: String): Map[String, String] =
      info
        .split("\\r?\\n")
        .toList
        .map(_.split(":", 2).toList)
        .collect { case k :: v :: Nil => (k, v) }
        .toMap

    Try {
      //Temporary client just to get Redis INFO
      val singleNodeRedisClient = JRedisClient.create(
        RedisURI.builder()
          .withHost(redisClientConfig.host)
          .withPort(redisClientConfig.port)
          .build()
      )

      //Issuing INFO command (synchronously)
      val redisInfo = parseInfo(singleNodeRedisClient.connect().sync.info())

      //Closing temporary client
      singleNodeRedisClient.close()

      val isClusterEnabled = redisInfo("cluster_enabled") == "1"
      logger.info(s"Bootstrap Redis client determined from INFO command that cluster is ${if (isClusterEnabled) "enabled" else "disabled"} for host/port = ${redisClientConfig.host}/${redisClientConfig.port}")
      isClusterEnabled
    }
  }
}

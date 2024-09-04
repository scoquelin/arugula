package com.github.scoquelin.arugula

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._
import scala.util.{Failure, Success, Try}

import com.github.scoquelin.arugula.codec.RedisCodec
import com.github.scoquelin.arugula.commands._
import com.github.scoquelin.arugula.config.LettuceRedisClientConfig
import com.github.scoquelin.arugula.connection.RedisConnection
import com.github.scoquelin.arugula.internal.{LettuceRedisClusterClient, LettuceRedisCommandDelegation, LettuceRedisSingleNodeClient}
import io.lettuce.core.RedisFuture
import io.lettuce.core.cluster.api.async.{RedisClusterAsyncCommands => JRedisClusterAsyncCommands}

/**
 * This is the Lettuce implementation of the RedisAsyncCommands API.
 * This implementation is meant to evolve and any Redis operation that is supported by the Lettuce API could be potentially added here.
 *
 * @param connection The underlying Lettuce connection that will be used to relay commands.
 * @param cluster Flag explicitly conveying if we are if a cluster-enabled environment(client/server) or not
 * @param executionContext The execution context
 * @tparam K type of the key
 * @tparam V type of the value
 */
private[arugula] class LettuceRedisCommandsClient[K, V](
  connection: RedisConnection[K, V],
  cluster: Boolean,
  )(override private[arugula] implicit val executionContext: ExecutionContext)
  extends RedisCommandsClient[K, V]
    with LettuceRedisCommandDelegation[K, V]
    with LettuceRedisBaseAsyncCommands[K, V]
    with LettuceRedisKeyAsyncCommands[K, V]
    with LettuceRedisHashAsyncCommands[K, V]
    with LettuceRedisServerAsyncCommands[K, V]
    with LettuceRedisListAsyncCommands[K, V]
    with LettuceRedisScriptingAsyncCommands[K, V]
    with LettuceRedisStringAsyncCommands[K, V]
    with LettuceRedisSetAsyncCommands[K, V]
    with LettuceRedisSortedSetAsyncCommands[K, V]
    with LettuceRedisPipelineAsyncCommands[K, V] {


  //Generalizing over JRedisClusterAsyncCommands for both single-node and cluster commands as we don't need transaction commands (that are single-node only)
  //(relying on the fact that io.lettuce.core.api.async.RedisAsyncCommands also extends io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands)
  private val lettuceRedisClusterAsyncCommands: Future[JRedisClusterAsyncCommands[K, V]] =
    if (cluster) connection.clusterAsync else connection.async


  /**
   * Lifts the outcome (RedisFuture[T]) of a Lettuce Redis command into a Scala Future[T]
   *
   * @param command The Lettuce command to be dispatched
   * @tparam T The type of the returned value inside the io.lettuce.core.RedisFuture[T]
   * @return A Scala Future[T] extracted from the io.lettuce.core.RedisFuture[T]
   */
  override private[arugula] def delegateRedisClusterCommandAndLift[T](command: JRedisClusterAsyncCommands[K, V] => RedisFuture[T]): Future[T] =
    lettuceRedisClusterAsyncCommands.flatMap(redisCommands => command(redisCommands).toCompletableFuture.asScala)


}


object LettuceRedisCommandsClient {

  def apply(redisClientConfig: LettuceRedisClientConfig)(implicit ec: ExecutionContext): LettuceRedisCommandsClient[String, String] =
    apply(redisClientConfig, RedisCodec.Utf8WithValueAsStringCodec)

  def apply[K, V](redisClientConfig: LettuceRedisClientConfig, redisCodec: RedisCodec[K, V])(implicit ec: ExecutionContext): LettuceRedisCommandsClient[K, V] = {
    //Check if target Redis server is "cluster-enabled" by contacting Redis server using INFO and instantiate the appropriate client (single-node or cluster)
    checkIfClusterEnabled(redisClientConfig) match {
      case Success(clusterEnabled@true) =>
        new LettuceRedisCommandsClient(
          LettuceRedisClusterClient.apply(redisClientConfig).getRedisConnection(redisCodec),
          cluster = clusterEnabled
        )

      case Success(clusterDisabled@false) =>
        new LettuceRedisCommandsClient(
          LettuceRedisSingleNodeClient.apply(redisClientConfig).getRedisConnection(redisCodec),
          cluster = clusterDisabled
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
      isClusterEnabled
    }
  }
}
package com.github.scoquelin.arugula

import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import com.github.scoquelin.arugula.BaseRedisCommandsIntegrationSpec._
import com.github.scoquelin.arugula.codec.RedisCodec
import com.github.scoquelin.arugula.config.LettuceRedisClientConfig
import io.lettuce.core.internal.HostAndPort
import io.lettuce.core.resource.{ClientResources, DnsResolvers, MappingSocketAddressResolver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AsyncWordSpecLike
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File
import scala.concurrent.Future
import scala.jdk.FunctionConverters.enrichAsJavaFunction

trait BaseRedisCommandsIntegrationSpec extends AsyncWordSpecLike with TestContainerForAll with BeforeAndAfterEach {
  var cachedClients: CachedClients = null

  override val containerDef: DockerComposeContainer.Def = {
    DockerComposeContainer.Def(
      composeFiles = new File("src/test/resources/docker-compose.yml"),
      exposedServices = Seq(
        ExposedService(RedisSingleNode, RedisSingleNodePort, Wait.forLogMessage(".*Ready to accept connections.*", 1)),
        ExposedService(RedisClusterNode, RedisClusterPort, Wait.forLogMessage(".*Background AOF rewrite finished successfully.*", 1))
      )
    )
  }

  override def afterContainersStart(containers: Containers): Unit = {
    super.afterContainersStart(containers)

    val redisSingleNodeConfig = LettuceRedisClientConfig(
      host = containers.getServiceHost(RedisSingleNode, RedisSingleNodePort),
      port = containers.getServicePort(RedisSingleNode, RedisSingleNodePort)
    )

    //Special hack to get cluster client topology refresh working since we need direct connectivity to cluster nodes see https://github.com/lettuce-io/lettuce-core/issues/941
    val mapHostAndPort: HostAndPort => HostAndPort = hostAndPort => {
      if (hostAndPort.getHostText.startsWith("172.") || hostAndPort.getHostText.startsWith("10.") || hostAndPort.getHostText.startsWith("192.")) {
        HostAndPort.of("localhost", hostAndPort.getPort)
      } else {
        hostAndPort
      }
    }

    val resolver = MappingSocketAddressResolver.create(DnsResolvers.UNRESOLVED, mapHostAndPort.asJavaFunction)
    val clientResources = ClientResources.builder.socketAddressResolver(resolver).build

    val redisClusterConfig = LettuceRedisClientConfig(
        host = containers.getServiceHost(RedisClusterNode, RedisClusterPort),
        port = containers.getServicePort(RedisClusterNode, RedisClusterPort),
        clientResources = clientResources
    )

    cachedClients = RedisCommandCachedClients(redisSingleNodeConfig, redisClusterConfig)
  }

  override def afterEach(): Unit = {
    //flushing both redis instances after each test
    cachedClients.getClient(RedisCodec.Utf8WithValueAsStringCodec, SingleNode).flushAll
    cachedClients.getClient(RedisCodec.Utf8WithValueAsStringCodec, Cluster).flushAll
  }

  def withRedisSingleNode[K, V, A](codec: RedisCodec[K, V])(runTest: RedisCommandsClient[K, V] => Future[A]): Future[A] = {
    withContainers(_ => {
      runTest(cachedClients.getClient(codec, SingleNode))
    })
  }

  def withRedisCluster[K, V, A](codec: RedisCodec[K, V])(runTest: RedisCommandsClient[K, V] => Future[A]): Future[A] = {
    withContainers(_ => {
      runTest(cachedClients.getClient(codec, Cluster))
    })
  }

  def withRedisSingleNodeAndCluster[K, V, A](codec: RedisCodec[K, V])(runTest: RedisCommandsClient[K, V] => Future[A]): Future[A] = {
    withContainers(_ => {
      for {
        _ <- runTest(cachedClients.getClient(codec, SingleNode))
        outcome <- runTest(cachedClients.getClient(codec, Cluster))
      } yield outcome
    })
  }

}

object BaseRedisCommandsIntegrationSpec {
  val RedisSingleNode = "redis-single-node"
  val RedisSingleNodePort = 6379

  val RedisClusterNode = "redis-cluster"
  val RedisClusterPort = 7005
}

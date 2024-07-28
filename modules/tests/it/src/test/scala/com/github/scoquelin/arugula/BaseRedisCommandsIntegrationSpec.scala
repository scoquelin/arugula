package com.github.scoquelin.arugula

import com.github.scoquelin.arugula.BaseRedisCommandsIntegrationSpec._
import com.github.scoquelin.arugula.config.LettuceRedisClientConfig
import scala.jdk.FunctionConverters.enrichAsJavaFunction

import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.{DockerComposeContainer, ExposedService}
import io.lettuce.core.internal.HostAndPort
import io.lettuce.core.resource.{ClientResources, DnsResolvers, MappingSocketAddressResolver}
import org.scalatest.wordspec.AsyncWordSpecLike
import org.scalatest.{BeforeAndAfterEach, Suite}
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File

trait BaseRedisCommandsIntegrationSpec extends AsyncWordSpecLike with TestContainerForAll with BeforeAndAfterEach { self: Suite =>
  var redisSingleNodeCommandsClient: RedisCommandsClient[String, String] = null
  var redisClusterCommandsClient: RedisCommandsClient[String, String] = null

  override val containerDef = {
    DockerComposeContainer.Def(
      composeFiles = new File("src/test/resources/docker-compose.yml"),
      exposedServices = Seq(
        ExposedService(RedisSingleNode, RedisSingleNodePort, Wait.forLogMessage(".*Ready to accept connections.*", 1)),
        ExposedService(RedisClusterNode, RedisClusterPort, Wait.forLogMessage(".*Background AOF rewrite finished successfully.*", 1))
      )
    )
  }

  override def afterContainersStart(container: Containers): Unit = {
    super.afterContainersStart(container)

    redisSingleNodeCommandsClient = LettuceRedisCommandsClient(
      LettuceRedisClientConfig(
        container.getServiceHost(RedisSingleNode, RedisSingleNodePort),
        container.getServicePort(RedisSingleNode, RedisSingleNodePort)
      )
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

    redisClusterCommandsClient = LettuceRedisCommandsClient(
      LettuceRedisClientConfig(
        container.getServiceHost(RedisClusterNode, RedisClusterPort),
        container.getServicePort(RedisClusterNode, RedisClusterPort),
        clientResources
      )
    )
  }

  override def afterEach(): Unit = {
    //flushing both redis instances after each test
    redisSingleNodeCommandsClient.flushAll
    redisClusterCommandsClient.flushAll
  }

  def withRedisSingleNode[K, V, A](runTest: RedisCommandsClient[String, String] => A): A = {
    withContainers(_ => runTest(redisSingleNodeCommandsClient))
  }

  def withRedisCluster[K, V, A](runTest: RedisCommandsClient[String, String] => A): A = {
    withContainers(_ => runTest(redisClusterCommandsClient))
  }

  def withRedisSingleNodeAndCluster[K, V, A](runTest: RedisCommandsClient[String, String] => A): A = {
    withContainers(_ => {
      runTest(redisSingleNodeCommandsClient)
      runTest(redisClusterCommandsClient)
    })
  }

}

object BaseRedisCommandsIntegrationSpec {
  val RedisSingleNode = "redis-single-node"
  val RedisSingleNodePort = 6379

  val RedisClusterNode = "redis-cluster"
  val RedisClusterPort = 7005
}

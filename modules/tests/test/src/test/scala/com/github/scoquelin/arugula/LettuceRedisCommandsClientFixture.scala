package com.github.scoquelin.arugula

import scala.concurrent.{ExecutionContext, Future}

import com.github.scoquelin.arugula.connection.RedisConnection
import io.lettuce.core.RedisFuture
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.CompletableFuture

object LettuceRedisCommandsClientFixture {
  import io.lettuce.core.api.async.{RedisAsyncCommands => JRedisAsyncCommands}


  class Mocks extends MockitoSugar {
    val redisConnection: RedisConnection[String, String] = mock[RedisConnection[String, String]]
    val lettuceAsyncCommands: JRedisAsyncCommands[String, String] = mock[JRedisAsyncCommands[String, String]]
    def redisFuture[T]: RedisFuture[T] = mock[RedisFuture[T]]
  }

  class TestContext(implicit executionContext: ExecutionContext) {
    val mocks = new Mocks
    val redisConnection: RedisConnection[String, String] = mocks.redisConnection
    val lettuceAsyncCommands: JRedisAsyncCommands[String, String] = mocks.lettuceAsyncCommands

    def mockRedisFutureToReturn[T](value: T): RedisFuture[T] = {
      val redisFuture = mocks.redisFuture[T]
      val completableFuture = new CompletableFuture[T]()
      completableFuture.complete(value)
      when(redisFuture.toCompletableFuture).thenReturn(completableFuture)
      redisFuture
    }

    when(redisConnection.async).thenReturn(Future.successful(lettuceAsyncCommands))

    val testClass = new LettuceRedisCommandsClient[String, String](redisConnection, cluster = false)
  }

}

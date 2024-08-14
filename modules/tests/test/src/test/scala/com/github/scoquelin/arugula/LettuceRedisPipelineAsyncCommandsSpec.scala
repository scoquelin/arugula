package com.github.scoquelin.arugula

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import io.lettuce.core.RedisFuture
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

class LettuceRedisPipelineAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisPipelineAsyncCommands" should {

    "send a batch of commands WITH NO transaction guarantees" in { testContext =>
      import testContext._

      val mockHsetRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(true)
      when(lettuceAsyncCommands.hset("userKey", "sessionId", "token")).thenReturn(mockHsetRedisFuture)

      val mockExpireRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(true)
      when(lettuceAsyncCommands.expire("userKey", 24.hours.toSeconds)).thenReturn(mockExpireRedisFuture)

      val commands: RedisCommandsClient[String, String] => List[Future[Any]] = availableCommands => List(
        availableCommands.hSet("userKey", "sessionId", "token"),
        availableCommands.expire("userKey", 24.hours)
      )

      testClass.pipeline(commands).map { results =>
        results mustBe Some(List(true, true))
        verify(lettuceAsyncCommands).hset("userKey", "sessionId", "token")
        verify(lettuceAsyncCommands).expire("userKey", 24.hours.toSeconds)
        succeed
      }
    }
  }

}

package com.github.scoquelin.arugula

import io.lettuce.core.RedisFuture
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

class LettuceRedisServerAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisServerAsyncCommands" should {
    "delegate INFO command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "redis_version:7.2.3"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.info()).thenReturn(mockRedisFuture)

      testClass.info.map { result =>
        result mustBe Map("redis_version" -> "7.2.3")
        verify(lettuceAsyncCommands).info()
        succeed
      }
    }
  }

}

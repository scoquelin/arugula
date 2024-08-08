package com.github.scoquelin.arugula

import io.lettuce.core.RedisFuture
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}


class LettuceRedisBaseAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisBaseAsyncCommands" should {

    "delegate PING command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "PONG"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.ping()).thenReturn(mockRedisFuture)

      testClass.ping.map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).ping()
        succeed
      }
    }
  }
}

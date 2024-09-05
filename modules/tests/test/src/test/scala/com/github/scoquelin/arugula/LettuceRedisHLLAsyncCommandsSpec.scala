package com.github.scoquelin.arugula

import io.lettuce.core.RedisFuture
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

class LettuceRedisHLLAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisHLLAsyncCommands" should {
    "delegate PFADD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.pfadd("key", "value")).thenReturn(mockRedisFuture)

      testClass.pfAdd("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).pfadd("key", "value")
        succeed
      }
    }

    "delegate PFMERGE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.pfmerge("destinationKey", "sourceKey")).thenReturn(mockRedisFuture)

      testClass.pfMerge("destinationKey", "sourceKey").map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).pfmerge("destinationKey", "sourceKey")
        succeed
      }
    }

    "delegate PFCOUNT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.pfcount("key")).thenReturn(mockRedisFuture)

      testClass.pfCount("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).pfcount("key")
        succeed
      }
    }
  }
}

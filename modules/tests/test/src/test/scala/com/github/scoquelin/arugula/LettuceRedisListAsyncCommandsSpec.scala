package com.github.scoquelin.arugula

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.commands.RedisListAsyncCommands
import io.lettuce.core.{KeyValue, RedisFuture}
import org.mockito.ArgumentMatchers.{any, anyDouble, anyString, eq => meq}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

import java.util.concurrent.TimeUnit

class LettuceRedisListAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisListAsyncCommands" should {
    "delegate BLMOVE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.blmove(any, any, any, anyDouble)).thenReturn(mockRedisFuture)

      testClass.blMove("source", "destination", RedisListAsyncCommands.Side.Left, RedisListAsyncCommands.Side.Right, FiniteDuration(1, TimeUnit.SECONDS)).map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).blmove(meq("source"), meq("destination"), any, meq(1.0))
        succeed
      }
    }

    "delegate BLMPOP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = KeyValue.fromNullable("key", List("value").asJava)
      val mockRedisFuture: RedisFuture[KeyValue[String, java.util.List[String]]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.blmpop(anyDouble, any, anyString)).thenReturn(mockRedisFuture)
      testClass.blMPop(List("key"), timeout=FiniteDuration(1, TimeUnit.SECONDS)).map { result =>
        verify(lettuceAsyncCommands).blmpop(meq(1.0), any, meq("key"))
        result mustBe Some(("key", List("value")))
        succeed
      }
    }

    "delegate LINSERT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.linsert("key", true, "value", "pivot")).thenReturn(mockRedisFuture)

      testClass.lInsert("key", before = true, pivot = "value", value = "pivot").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).linsert("key", true, "value", "pivot")
        succeed
      }
    }

    "delegate LMPOP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = KeyValue.fromNullable("key", List("value").asJava)
      val mockRedisFuture: RedisFuture[KeyValue[String, java.util.List[String]]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.lmpop(any, anyString)).thenReturn(mockRedisFuture)
      testClass.lMPop(List("key")).map { result =>
        verify(lettuceAsyncCommands).lmpop(any, meq("key"))
        result mustBe Some(("key", List("value")))
        succeed
      }
    }

    "delegate LPUSH command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.lpush("key", "value")).thenReturn(mockRedisFuture)

      testClass.lPush("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).lpush("key", "value")
        succeed
      }
    }

    "delegate LPUSHX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.lpushx("key", "value")).thenReturn(mockRedisFuture)

      testClass.lPushX("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).lpushx("key", "value")
        succeed
      }
    }

    "delegate LPOP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.lpop("key")).thenReturn(mockRedisFuture)

      testClass.lPop("key").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).lpop("key")
        succeed
      }
    }

    "delegate BRPOP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = KeyValue.fromNullable("key", "value")
      val mockRedisFuture: RedisFuture[KeyValue[String, String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.brpop(anyDouble, anyString)).thenReturn(mockRedisFuture)

      testClass.brPop(FiniteDuration(1, TimeUnit.SECONDS), "key").map { result =>
        result mustBe Some(("key", "value"))
        verify(lettuceAsyncCommands).brpop(meq(1.0), meq("key"))
        succeed
      }
    }

    "delegate RPOP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.rpop("key")).thenReturn(mockRedisFuture)

      testClass.rPop("key").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).rpop("key")
        succeed
      }
    }

    "delegate RPOPLPUSH command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.rpoplpush("source", "destination")).thenReturn(mockRedisFuture)

      testClass.rPopLPush("source", "destination").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).rpoplpush("source", "destination")
        succeed
      }
    }

    "delegate LINDEX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.lindex("key", 0)).thenReturn(mockRedisFuture)

      testClass.lIndex("key", 0).map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).lindex("key", 0)
        succeed
      }
    }

    "delegate LLEN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.llen("key")).thenReturn(mockRedisFuture)

      testClass.lLen("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).llen("key")
        succeed
      }
    }

    "delegate LMOVE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.lmove(any, any, any)).thenReturn(mockRedisFuture)

      testClass.lMove("source", "destination", RedisListAsyncCommands.Side.Left, RedisListAsyncCommands.Side.Right).map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).lmove(meq("source"), meq("destination"), any)
        succeed
      }
    }

    "delegate LPOS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.lpos("key", "value")).thenReturn(mockRedisFuture)

      testClass.lPos("key", "value").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).lpos("key", "value")
        succeed
      }
    }

    "delegate LREM command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.lrem("key", 1, "value")).thenReturn(mockRedisFuture)

      testClass.lRem("key", 1, "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).lrem("key", 1, "value")
        succeed
      }
    }

    "delegate LSET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")

      when(lettuceAsyncCommands.lset("key", 0, "value")).thenReturn(mockRedisFuture)

      testClass.lSet("key", 0, "value").map { _ =>
        verify(lettuceAsyncCommands).lset("key", 0, "value")
        succeed
      }
    }

    "delegate LTRIM command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")

      when(lettuceAsyncCommands.ltrim("key", 0, 1)).thenReturn(mockRedisFuture)

      testClass.lTrim("key", 0, 1).map { _ =>
        verify(lettuceAsyncCommands).ltrim("key", 0, 1)
        succeed
      }
    }

    "delegate RPUSH command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.rpush("key", "value")).thenReturn(mockRedisFuture)

      testClass.rPush("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).rpush("key", "value")
        succeed
      }
    }

    "delegate RPUSHX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.rpushx("key", "value")).thenReturn(mockRedisFuture)

      testClass.rPushX("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).rpushx("key", "value")
        succeed
      }
    }
  }
}

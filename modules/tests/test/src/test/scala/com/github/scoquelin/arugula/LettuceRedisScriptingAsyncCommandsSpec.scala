package com.github.scoquelin.arugula

import scala.jdk.CollectionConverters.CollectionHasAsScala

import com.github.scoquelin.arugula.commands.RedisScriptingAsyncCommands
import io.lettuce.core.{RedisFuture, ScriptOutputType}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

class LettuceRedisScriptingAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisScriptingAsyncCommands" should {
    "delegate EVAL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "Hello, world!"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.eval[String]("script", ScriptOutputType.VALUE, "key")).thenReturn(mockRedisFuture)

      testClass.eval("script", RedisScriptingAsyncCommands.ScriptOutputType.Value, "key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).eval("script", ScriptOutputType.VALUE, "key")
        succeed
      }
    }

    "delegate EVAL command with values to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "Hello, world!"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.eval[String]("script", ScriptOutputType.VALUE, List("key").toArray, "value")).thenReturn(mockRedisFuture)

      testClass.eval("script", RedisScriptingAsyncCommands.ScriptOutputType.Value, List("key"), "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).eval("script", ScriptOutputType.VALUE, List("key").toArray, "value")
        succeed
      }
    }

    "delegate EVALSHA command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "Hello, world!"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.evalsha[String]("sha", ScriptOutputType.VALUE, "key")).thenReturn(mockRedisFuture)

      testClass.evalSha("sha", RedisScriptingAsyncCommands.ScriptOutputType.Value, "key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).evalsha("sha", ScriptOutputType.VALUE, "key")
        succeed
      }
    }

    "delegate EVALSHA command with values to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "Hello, world!"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.evalsha[String]("sha", ScriptOutputType.VALUE, List("key").toArray, "value")).thenReturn(mockRedisFuture)

      testClass.evalSha("sha", RedisScriptingAsyncCommands.ScriptOutputType.Value, List("key"), "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).evalsha("sha", ScriptOutputType.VALUE, List("key").toArray, "value")
        succeed
      }
    }

    "delegate EVAL command with Array[Byte] script to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "Hello, world!"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.eval[String]("script".getBytes, ScriptOutputType.VALUE, "key")).thenReturn(mockRedisFuture)

      testClass.eval("script".getBytes, RedisScriptingAsyncCommands.ScriptOutputType.Value, "key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).eval("script".getBytes, ScriptOutputType.VALUE, "key")
        succeed
      }
    }

    "delegate EVAL command with Array[Byte] script and values to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "Hello, world!"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.eval[String]("script".getBytes, ScriptOutputType.VALUE, List("key").toArray, "value")).thenReturn(mockRedisFuture)

      testClass.eval("script".getBytes, RedisScriptingAsyncCommands.ScriptOutputType.Value, List("key"), "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).eval("script".getBytes, ScriptOutputType.VALUE, List("key").toArray, "value")
        succeed
      }
    }

    "delegate EVAL_RO command with Array[Byte] script and values to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "Hello, world!"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.evalReadOnly[String]("script".getBytes, ScriptOutputType.VALUE, List("key").toArray, "value")).thenReturn(mockRedisFuture)

      testClass.evalReadOnly("script".getBytes, RedisScriptingAsyncCommands.ScriptOutputType.Value, List("key"), "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).evalReadOnly("script".getBytes, ScriptOutputType.VALUE, List("key").toArray, "value")
        succeed
      }
    }

    "delegate EVAL_RO command with Array[Byte] script to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "Hello, world!"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.evalReadOnly[String]("script".getBytes, ScriptOutputType.VALUE, List("key").toArray)).thenReturn(mockRedisFuture)

      testClass.evalReadOnly("script".getBytes, RedisScriptingAsyncCommands.ScriptOutputType.Value, List("key")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).evalReadOnly("script".getBytes, ScriptOutputType.VALUE, List("key").toArray)
        succeed
      }
    }

    "delegate SCRIPT EXISTS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of(java.lang.Boolean.TRUE)
      val mockRedisFuture: RedisFuture[java.util.List[java.lang.Boolean]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.scriptExists("sha1")).thenReturn(mockRedisFuture)

      testClass.scriptExists("sha1").map { result =>
        result mustBe true
        verify(lettuceAsyncCommands).scriptExists("sha1")
        succeed
      }
    }

    "delegate SCRIPT EXISTS command to Lettuce with multiple shas and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of(java.lang.Boolean.TRUE, java.lang.Boolean.FALSE)
      val mockRedisFuture: RedisFuture[java.util.List[java.lang.Boolean]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.scriptExists("sha1", "sha2")).thenReturn(mockRedisFuture)

      testClass.scriptExists(List("sha1", "sha2")).map { result =>
        result mustBe expectedValue.asScala.map(_.booleanValue()).toList
        verify(lettuceAsyncCommands).scriptExists("sha1", "sha2")
        succeed
      }
    }

    "delegate SCRIPT FLUSH command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.scriptFlush()).thenReturn(mockRedisFuture)

      testClass.scriptFlush.map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).scriptFlush()
        succeed
      }
    }

    "delegate SCRIPT FLUSH ASYNC command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.scriptFlush(io.lettuce.core.FlushMode.ASYNC)).thenReturn(mockRedisFuture)

      testClass.scriptFlush(RedisScriptingAsyncCommands.FlushMode.Async).map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).scriptFlush(io.lettuce.core.FlushMode.ASYNC)
        succeed
      }
    }

    "delegate SCRIPT KILL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.scriptKill()).thenReturn(mockRedisFuture)

      testClass.scriptKill.map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).scriptKill()
        succeed
      }
    }

    "delegate SCRIPT LOAD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "sha1"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.scriptLoad("script")).thenReturn(mockRedisFuture)

      testClass.scriptLoad("script").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).scriptLoad("script")
        succeed
      }
    }

    "delegate SCRIPT LOAD command with bytes to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "sha1"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.scriptLoad("script".getBytes)).thenReturn(mockRedisFuture)

      testClass.scriptLoad("script".getBytes).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).scriptLoad("script".getBytes)
        succeed
      }
    }
  }

}

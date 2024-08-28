package com.github.scoquelin.arugula

import scala.concurrent.duration.{DurationInt, DurationLong}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}

import io.lettuce.core.RedisFuture
import org.mockito.ArgumentMatchers.{anyInt, anyLong, anyString, any, eq => meq}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

import java.time.Instant

class LettuceRedisKeyAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisKeyAsyncCommands" should {
    "delegate COPY command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(java.lang.Boolean.TRUE)
      when(lettuceAsyncCommands.copy(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.copy("srcKey", "destKey").map { result =>
        result mustBe true
        verify(lettuceAsyncCommands).copy("srcKey", "destKey")
        succeed
      }
    }

    "delegate DEL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.del(anyString)).thenReturn(mockRedisFuture)

      testClass.del("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).del("key")
        succeed
      }
    }

    "delegate UNLINK command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.unlink(anyString)).thenReturn(mockRedisFuture)

      testClass.unlink("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).unlink("key")
        succeed
      }
    }

    "delegate DEL command with multiple keys to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.del(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.del("key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).del("key1", "key2")
        succeed
      }
    }

    "delegate DUMP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Array.emptyByteArray
      val mockRedisFuture: RedisFuture[Array[Byte]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.dump(anyString)).thenReturn(mockRedisFuture)

      testClass.dump("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).dump("key")
        succeed
      }
    }


    "delegate EXISTS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.exists(anyString)).thenReturn(mockRedisFuture)

      testClass.exists("key").map { result =>
        result mustBe true
        verify(lettuceAsyncCommands).exists("key")
        succeed
      }
    }

    "delegate EXISTS command with multiple keys to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.exists(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.exists("key1", "key2").map { result =>
        result mustBe true
        verify(lettuceAsyncCommands).exists("key1", "key2")
        succeed
      }
    }

    "delegate expire to PEXPIRE when provided unit is MILLIS, MICROS or NANOS and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.pexpire(anyString, anyLong)).thenReturn(mockRedisFuture)

      testClass.expire("key", 1000.microseconds).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).pexpire("key", 1L)
        succeed
      }
    }

    "delegate expire to EXPIRE when provided unit is SECONDS or higher and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.expire(anyString, anyLong)).thenReturn(mockRedisFuture)

      testClass.expire("key", 1.minute).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).expire("key", 60L)
        succeed
      }
    }

    "delegate PEXPIREAT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.pexpireat(anyString, anyLong)).thenReturn(mockRedisFuture)

      testClass.expireAt("key", Instant.ofEpochMilli(1)).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).pexpireat("key", 1L)
        succeed
      }
    }

    "delegate PEXPIRETIME command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1000L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.pexpiretime(anyString)).thenReturn(mockRedisFuture)

      testClass.expireTime("key").map {
        case Some(result) =>
          result mustBe Instant.ofEpochMilli(expectedValue)
          verify(lettuceAsyncCommands).pexpiretime("key")
          succeed
        case None =>
          fail(s"PEXPIRETIME for key should be $expectedValue")
      }
    }

    "delegate KEYS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("key1", "key2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.keys(anyString)).thenReturn(mockRedisFuture)
      testClass.keys("pattern").map { result =>
        result mustBe expectedValue.asScala.toList
        verify(lettuceAsyncCommands).keys("pattern")
        succeed
      }
    }

    "delegate MOVE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.move(anyString, anyInt)).thenReturn(mockRedisFuture)

      testClass.move("key", 1).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).move("key", 1)
        succeed
      }
    }

    "delegate RENAME command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.rename(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.rename("key", "newKey").map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).rename("key", "newKey")
        succeed
      }
    }

    "delegate RENAMENX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.renamenx(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.renameNx("key", "newKey").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).renamenx("key", "newKey")
        succeed
      }
    }

    "delegate RESTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.restore(anyString, any[Array[Byte]], any[io.lettuce.core.RestoreArgs])).thenReturn(mockRedisFuture)

      testClass.restore("key", Array.emptyByteArray).map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).restore(meq("key"), any[Array[Byte]], any[io.lettuce.core.RestoreArgs])
        succeed
      }
    }

    "delegate SCAN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = new io.lettuce.core.KeyScanCursor[String]
      expectedValue.setFinished(true)
      expectedValue.setCursor("0")
      val mockRedisFuture: RedisFuture[io.lettuce.core.KeyScanCursor[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.scan(any[io.lettuce.core.ScanCursor])).thenReturn(mockRedisFuture)

      testClass.scan().map { result =>
        result.cursor mustBe "0"
        result.finished mustBe true
        result.values mustBe empty
        verify(lettuceAsyncCommands).scan(any[io.lettuce.core.ScanCursor])
        succeed
      }
    }

    "delegate SCAN command with arguments to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = new io.lettuce.core.KeyScanCursor[String]
      expectedValue.setFinished(true)
      expectedValue.setCursor("0")
      val mockRedisFuture: RedisFuture[io.lettuce.core.KeyScanCursor[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.scan(any[io.lettuce.core.ScanCursor], any[io.lettuce.core.ScanArgs])).thenReturn(mockRedisFuture)

      testClass.scan("0", Some("pattern"), Some(10)).map { result =>
        result.cursor mustBe "0"
        result.finished mustBe true
        result.values mustBe empty
        verify(lettuceAsyncCommands).scan(any[io.lettuce.core.ScanCursor], any[io.lettuce.core.ScanArgs])
        succeed
      }
    }

    "delegate PTTL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1000L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.exists(anyString, anyString)).thenReturn(mockRedisFuture)

      when(lettuceAsyncCommands.pttl(anyString)).thenReturn(mockRedisFuture)
      testClass.ttl("key").map {
        case Some(result) =>
          result mustBe 1.seconds
          verify(lettuceAsyncCommands).pttl("key")
          succeed
        case None =>
          fail(s"PTTL for key should be $expectedValue second(s)")
      }
    }

    "delegate TOUCH command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.touch(anyString)).thenReturn(mockRedisFuture)

      testClass.touch("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).touch("key")
        succeed
      }
    }


    "delegate TYPE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "string"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.`type`(anyString)).thenReturn(mockRedisFuture)

      testClass.`type`("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).`type`("key")
        succeed
      }
    }
  }

}

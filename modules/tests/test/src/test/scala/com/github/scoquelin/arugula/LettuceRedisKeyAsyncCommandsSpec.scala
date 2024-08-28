package com.github.scoquelin.arugula

import scala.concurrent.duration.{DurationInt, DurationLong}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}

import com.github.scoquelin.arugula.commands.RedisKeyAsyncCommands
import io.lettuce.core.RedisFuture
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, anyString, eq => meq}
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

    "delegate MIGRATE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.migrate(anyString, anyInt, anyString, anyInt, anyLong)).thenReturn(mockRedisFuture)

      testClass.migrate("host", 1, "key", 2, 1000.milliseconds).map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).migrate("host", 1, "key", 2, 1000)
        succeed
      }
    }

    "delegate MIGRATE command with arguments to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.migrate(anyString, anyInt, anyInt, anyLong, any[io.lettuce.core.MigrateArgs[String]])).thenReturn(mockRedisFuture)

      testClass.migrate("host", 1, 2, 1000.milliseconds, RedisKeyAsyncCommands.MigrationArgs[String](keys = List("key1", "key2"))).map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).migrate(meq("host"), meq(1), meq(2), meq(1000L), any[io.lettuce.core.MigrateArgs[String]])
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

    "delegate OBJECT ENCODING command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "encoding"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.objectEncoding(anyString)).thenReturn(mockRedisFuture)

      testClass.objectEncoding("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).objectEncoding("key")
        succeed
      }
    }

    "delegate OBJECT FREQUENCY command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.objectFreq(anyString)).thenReturn(mockRedisFuture)

      testClass.objectFreq("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).objectFreq("key")
        succeed
      }
    }

    "delegate OBJECT IDLETIME command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.objectIdletime(anyString)).thenReturn(mockRedisFuture)

      testClass.objectIdleTime("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).objectIdletime("key")
        succeed
      }
    }

    "delegate OBJECT REFCOUNT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.objectRefcount(anyString)).thenReturn(mockRedisFuture)

      testClass.objectRefCount("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).objectRefcount("key")
        succeed
      }
    }

    "delegate RANDOMKEY command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "key"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.randomkey()).thenReturn(mockRedisFuture)

      testClass.randomKey().map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).randomkey()
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

    "delegate SORT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sort(anyString)).thenReturn(mockRedisFuture)

      testClass.sort("key").map { result =>
        result mustBe expectedValue.asScala.toList
        verify(lettuceAsyncCommands).sort("key")
        succeed
      }
    }

    "delegate SORT command with arguments to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sort(anyString, any[io.lettuce.core.SortArgs])).thenReturn(mockRedisFuture)

      testClass.sort("key", RedisKeyAsyncCommands.SortArgs(by = Option("test*"))).map { result =>
        result mustBe expectedValue.asScala.toList
        verify(lettuceAsyncCommands).sort(meq("key"), any[io.lettuce.core.SortArgs])
        succeed
      }
    }

    "delegate SORT_RO command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sortReadOnly(anyString)).thenReturn(mockRedisFuture)

      testClass.sortReadOnly("key").map { result =>
        result mustBe expectedValue.asScala.toList
        verify(lettuceAsyncCommands).sortReadOnly("key")
        succeed
      }
    }

    "delegate SORT_RO command with arguments to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sortReadOnly(anyString, any[io.lettuce.core.SortArgs])).thenReturn(mockRedisFuture)

      testClass.sortReadOnly("key", RedisKeyAsyncCommands.SortArgs(by = Option("test*"))).map { result =>
        result mustBe expectedValue.asScala.toList
        verify(lettuceAsyncCommands).sortReadOnly(meq("key"), any[io.lettuce.core.SortArgs])
        succeed
      }
    }

    "delegate SORTSTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sortStore(anyString, any[io.lettuce.core.SortArgs], anyString)).thenReturn(mockRedisFuture)

      testClass.sortStore("key", "destKey", RedisKeyAsyncCommands.SortArgs(by = Option("test*"))).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sortStore(meq("key"), any[io.lettuce.core.SortArgs], meq("destKey"))
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

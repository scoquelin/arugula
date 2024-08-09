package com.github.scoquelin.arugula

import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands.{InitialCursor, ScanResults}
import io.lettuce.core.{RedisFuture, ValueScanCursor}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

class LettuceRedisSetAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisSetAsyncCommands" should {
    "delegate SADD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Long = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sadd(any, any)).thenReturn(mockRedisFuture)

      testClass.sAdd("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sadd("key", "value")
        succeed
      }
    }

    "delegate SCARD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Long = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.scard(any)).thenReturn(mockRedisFuture)

      testClass.sCard("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).scard("key")
        succeed
      }
    }

    "delegate SDIFF command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Set("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.Set[String]] = mockRedisFutureToReturn(expectedValue.asJava)
      when(lettuceAsyncCommands.sdiff(any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sDiff("key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sdiff("key1", "key2")
        succeed
      }
    }

    "delegate SDIFFSTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Long = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sdiffstore(any[String], any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sDiffStore("destination", "key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sdiffstore("destination", "key1", "key2")
        succeed
      }
    }

    "delegate SINTER command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Set("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.Set[String]] = mockRedisFutureToReturn(expectedValue.asJava)
      when(lettuceAsyncCommands.sinter(any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sInter("key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sinter("key1", "key2")
        succeed
      }
    }

    "delegate SINTERCARD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Long = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sintercard(any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sInterCard("key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sintercard("key1", "key2")
        succeed
      }
    }

    "delegate SINTERSTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Long = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sinterstore(any[String], any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sInterStore("destination", "key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sinterstore("destination", "key1", "key2")
        succeed
      }
    }

    "delegate SISMEMBER command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Boolean = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sismember(any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sIsMember("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sismember("key", "value")
        succeed
      }
    }

    "delegate SMEMBERS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Set("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.Set[String]] = mockRedisFutureToReturn(expectedValue.asJava)
      when(lettuceAsyncCommands.smembers(any[String])).thenReturn(mockRedisFuture)

      testClass.sMembers("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).smembers("key")
        succeed
      }
    }

    "delegate SMISMEMBER command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = List(true, false)
      val mockRedisFuture: RedisFuture[java.util.List[java.lang.Boolean]] = mockRedisFutureToReturn(expectedValue.map(Boolean.box).asJava)
      when(lettuceAsyncCommands.smismember(any[String], any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.smIsMember("key1", "value1", "value2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).smismember("key1", "value1", "value2")
        succeed
      }
    }

    "delegate SMOVE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Boolean = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.smove(any[String], any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sMove("source", "destination", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).smove("source", "destination", "value")
        succeed
      }
    }

    "delegate SPOP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.spop(any[String])).thenReturn(mockRedisFuture)

      testClass.sPop("key").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).spop("key")
        succeed
      }
    }

    "delegate SRANDMEMBER command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.srandmember(any[String])).thenReturn(mockRedisFuture)

      testClass.sRandMember("key").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).srandmember("key")
        succeed
      }
    }

    "delegate SRANDMEMBER command with count to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Set("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue.toList.asJava)
      when(lettuceAsyncCommands.srandmember(any[String], any[Int])).thenReturn(mockRedisFuture)

      testClass.sRandMember("key", 2).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).srandmember("key", 2)
        succeed
      }
    }

    "delegate SREM command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Long = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.srem(any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sRem("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).srem("key", "value")
        succeed
      }
    }

    "delegate SUNION command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Set("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.Set[String]] = mockRedisFutureToReturn(expectedValue.asJava)
      when(lettuceAsyncCommands.sunion(any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sUnion("key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sunion("key1", "key2")
        succeed
      }
    }

    "delegate SUNIONSTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Long = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.sunionstore(any[String], any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.sUnionStore("destination", "key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).sunionstore("destination", "key1", "key2")
        succeed
      }
    }

    "delegate SSCAN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = ScanResults(cursor = "1", finished = true, values = Set("value1", "value2"))
      val valueScanCursor = new ValueScanCursor[String]()
      valueScanCursor.setCursor(expectedValue.cursor)
      valueScanCursor.setFinished(expectedValue.finished)
      valueScanCursor.getValues.addAll(expectedValue.values.asJava)
      val mockRedisFuture: RedisFuture[ValueScanCursor[String]] = mockRedisFutureToReturn(valueScanCursor)
      when(lettuceAsyncCommands.sscan(any[String], any[io.lettuce.core.ScanCursor])).thenReturn(mockRedisFuture)

      testClass.sScan("key").map { result =>
        result mustBe expectedValue
        val cursorCaptor = ArgumentCaptor.forClass(classOf[io.lettuce.core.ScanCursor])
        verify(lettuceAsyncCommands).sscan(meq("key"), cursorCaptor.capture())
        val cursor = cursorCaptor.getValue.asInstanceOf[io.lettuce.core.ScanCursor]
        cursor.getCursor mustBe InitialCursor
        succeed
      }
    }
  }

}

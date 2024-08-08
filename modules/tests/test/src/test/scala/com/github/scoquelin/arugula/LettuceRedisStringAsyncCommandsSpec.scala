package com.github.scoquelin.arugula

import scala.collection.immutable.ListMap
import scala.concurrent.duration.DurationInt

import com.github.scoquelin.arugula.commands.RedisStringAsyncCommands.{BitFieldCommand, BitFieldDataType}
import io.lettuce.core.{GetExArgs, KeyValue, RedisFuture}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyDouble, anyLong, anyString, eq => meq}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}
import scala.jdk.CollectionConverters._

class LettuceRedisStringAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisStringAsyncCommands" should {
    "delegate APPEND command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 5L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.append(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.append("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).append("key", "value")
        succeed
      }
    }

    "delegate GET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.get(anyString)).thenReturn(mockRedisFuture)

      testClass.get("key").map {
        case Some(value) =>
          value mustBe expectedValue
          verify(lettuceAsyncCommands).get("key")
          succeed
        case None => fail(s"Value for GET(key) should be \"$expectedValue\"")
      }
    }

    "delegate GETDEL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.getdel(anyString)).thenReturn(mockRedisFuture)

      testClass.getDel("key").map {
        case Some(value) =>
          value mustBe expectedValue
          verify(lettuceAsyncCommands).getdel("key")
          succeed
        case None => fail(s"Value for GETDEL(key) should be \"$expectedValue\"")
      }
    }

    "delegate GETEX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.getex(anyString, any[GetExArgs])).thenReturn(mockRedisFuture)

      testClass.getEx("key", 1.second).map {
        case Some(value) =>
          value mustBe expectedValue
          verify(lettuceAsyncCommands).getex(meq("key"), any[GetExArgs])
          succeed
        case None => fail(s"Value for GETEX(key, 1.second) should be \"$expectedValue\"")
      }
    }

    "delegate GETSET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.getset(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.getSet("key", "value").map {
        case Some(value) =>
          value mustBe expectedValue
          verify(lettuceAsyncCommands).getset("key", "value")
          succeed
        case None => fail(s"Value for GETSET(key, value) should be \"$expectedValue\"")
      }
    }

    "delegate GETRANGE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.getrange(anyString, anyLong, anyLong)).thenReturn(mockRedisFuture)

      testClass.getRange("key", 0, 1).map {
        case Some(value) =>
          value mustBe expectedValue
          verify(lettuceAsyncCommands).getrange("key", 0, 1)
          succeed
        case None => fail(s"Value for GETRANGE(key, 0, 1) should be \"$expectedValue\"")
      }
    }

    "delegate MGET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: List[KeyValue[String, String]] = List(KeyValue.fromNullable("key1", "value1"), KeyValue.fromNullable("key2", "value2"), KeyValue.fromNullable("key3", null))
      val mockRedisFuture: RedisFuture[java.util.List[KeyValue[String, String]]] = mockRedisFutureToReturn(expectedValue.asJava)
      when(lettuceAsyncCommands.mget(anyString, anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.mGet("key1", "key2", "key3").map { result =>
        result mustBe ListMap("key1" -> Some("value1"), "key2" -> Some("value2"), "key3" -> None)
        verify(lettuceAsyncCommands).mget("key1", "key2", "key3")
        succeed
      }
    }

    "delegate MSET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.mset(any[java.util.Map[String, String]]())).thenReturn(mockRedisFuture)

      testClass.mSet(Map("key1" -> "value1", "key2" -> "value2")).map { _ =>
        verify(lettuceAsyncCommands).mset(Map("key1" -> "value1", "key2" -> "value2").asJava)
        succeed
      }
    }

    "delegate MSETNX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.msetnx(any[java.util.Map[String, String]]())).thenReturn(mockRedisFuture)

      testClass.mSetNx(Map("key1" -> "value1", "key2" -> "value2")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).msetnx(Map("key1" -> "value1", "key2" -> "value2").asJava)
        succeed
      }
    }

    "delegate SET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "OK"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.set(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.set("key", "value").map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).set("key", "value")
        succeed
      }
    }

    "delegate SETRANGE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 5L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.setrange(anyString, anyLong, anyString)).thenReturn(mockRedisFuture)

      testClass.setRange("key", 0, "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).setrange("key", 0, "value")
        succeed
      }
    }

    "delegate STRLEN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 5L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.strlen(anyString)).thenReturn(mockRedisFuture)

      testClass.strLen("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).strlen("key")
        succeed
      }
    }

    "delegate setex to PSETEX when provided unit is MILLIS, MICROS or NANOS and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "OK"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.psetex(anyString, anyLong, anyString)).thenReturn(mockRedisFuture)

      testClass.setEx("key", "value", 1000.microsecond).map { _ =>
        verify(lettuceAsyncCommands).psetex("key", 1L, "value")
        succeed
      }
    }

    "delegate setex to SETEX when provided unit is SECONDS or higher and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "OK"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.setex(anyString, anyLong, anyString)).thenReturn(mockRedisFuture)

      testClass.setEx("key", "value", 1.minute).map { _ =>
        verify(lettuceAsyncCommands).setex("key", 60L, "value")
        succeed
      }
    }

    "delegate SETNX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.setnx(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.setNx("key", "value").map { _ =>
        verify(lettuceAsyncCommands).setnx("key", "value")
        succeed
      }
    }

    "delegate INCR command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.incr(anyString)).thenReturn(mockRedisFuture)

      testClass.incr("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).incr("key")
        succeed
      }
    }

    "delegate INCRBY command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.incrby(anyString, anyLong)).thenReturn(mockRedisFuture)

      testClass.incrBy("key", 2).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).incrby("key", 2)
        succeed
      }
    }


    "delegate INCRBYFLOAT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2.5
      val mockRedisFuture: RedisFuture[java.lang.Double] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.incrbyfloat(anyString, any[Double])).thenReturn(mockRedisFuture)

      testClass.incrByFloat("key", 2.5).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).incrbyfloat("key", 2.5)
        succeed
      }
    }

    "delegate DECR command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.decr(anyString)).thenReturn(mockRedisFuture)

      testClass.decr("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).decr("key")
        succeed
      }
    }

    "delegate DECRBY command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.decrby(anyString, anyLong)).thenReturn(mockRedisFuture)

      testClass.decrBy("key", 2).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).decrby("key", 2)
        succeed
      }
    }

    "delegate BITCOUNT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.bitcount(anyString)).thenReturn(mockRedisFuture)

      testClass.bitCount("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitcount("key")
        succeed
      }
    }

    "delegate BITCOUNT command with start and end to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.bitcount(anyString, anyLong, anyLong)).thenReturn(mockRedisFuture)

      testClass.bitCount("key", 0, 1).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitcount("key", 0, 1)
        succeed
      }
    }

    "delegate BITFIELD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Seq[Long] = Seq(1L, 2L, 3L)
      val mockRedisFuture: RedisFuture[java.util.List[java.lang.Long]] = mockRedisFutureToReturn(expectedValue.map(v => v: java.lang.Long).asJava)
      when(lettuceAsyncCommands.bitfield(anyString, any[io.lettuce.core.BitFieldArgs])).thenReturn(mockRedisFuture)
      val commands: Seq[BitFieldCommand] = Seq(
        BitFieldCommand.get(BitFieldDataType.Signed(8)),
        BitFieldCommand.set(BitFieldDataType.Signed(8), 1L, offset = 1),
        BitFieldCommand.incrBy(BitFieldDataType.Signed(8), 1L, offset = 2)
      )
      testClass.bitField("key", commands).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitfield(meq("key"), any)
        succeed
      }
    }

    "delegate BITOPAND command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.bitopAnd(anyString, anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.bitOpAnd("destination", "key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitopAnd("destination", "key1", "key2")
        succeed
      }
    }

    "delegate BITOPOR command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.bitopOr(anyString, anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.bitOpOr("destination", "key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitopOr("destination", "key1", "key2")
        succeed
      }
    }

    "delegate BITOPXOR command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.bitopXor(anyString, anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.bitOpXor("destination", "key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitopXor("destination", "key1", "key2")
        succeed
      }
    }

    "delegate BITOPNOT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.bitopNot(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.bitOpNot("destination", "key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitopNot("destination", "key")
        succeed
      }
    }

    "delegate BITPOS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.bitpos(anyString, anyBoolean)).thenReturn(mockRedisFuture)

      testClass.bitPos("key", true).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitpos("key", true)
        succeed
      }
    }

    "delegate BITPOS command with start to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.bitpos(anyString, anyBoolean, anyLong)).thenReturn(mockRedisFuture)

      testClass.bitPos("key", true, 0).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitpos("key", true, 0)
        succeed
      }
    }

    "delegate BITPOS command with start and end to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.bitpos(anyString, anyBoolean, anyLong, anyLong)).thenReturn(mockRedisFuture)

      testClass.bitPos("key", true, 0, 1).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).bitpos("key", true, 0, 1)
        succeed
      }
    }
  }

}

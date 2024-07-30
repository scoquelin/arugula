package com.github.scoquelin.arugula

import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, DurationLong}
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands.{RangeLimit, ScoreWithValue, ZRange}
import com.github.scoquelin.arugula.connection.RedisConnection
import io.lettuce.core.{GetExArgs, KeyValue, RedisFuture, ScoredValue, ScoredValueScanCursor}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyLong, anyString, eq => meq}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.CompletableFuture

class LettuceRedisCommandsClientSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val testContext = new TestContext
    withFixture(test.toNoArgAsyncTest(testContext))
  }

  "LettuceRedisAsyncCommands" should {

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



    "delegate HGET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "value"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hget(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.hGet("key", "field").map {
        case Some(value) =>
          value mustBe expectedValue
          verify(lettuceAsyncCommands).hget("key", "field")
          succeed
        case None => fail(s"Value for HGET(key, field) should be \"$expectedValue\"")
      }
    }

    "delegate HGETALL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Map("field1" -> "value", "field2" -> "value")
      val mockRedisFuture: RedisFuture[java.util.Map[String, String]] = mockRedisFutureToReturn(expectedValue.asJava)
      when(lettuceAsyncCommands.hgetall(anyString)).thenReturn(mockRedisFuture)

      testClass.hGetAll("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hgetall("key")
        succeed
      }
    }

    "delegate HSET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hset(anyString, anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.hSet("key", "field", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hset("key", "field", "value")
        succeed
      }
    }

    "delegate HMSET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "OK"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hmset(anyString, any[java.util.Map[String, String]]())).thenReturn(mockRedisFuture)
      testClass.hMSet("key", Map("field1" -> "value1", "field2" -> "value2")).map { _ =>
        verify(lettuceAsyncCommands).hmset("key", Map("field1" -> "value1", "field2" -> "value2").asJava)
        succeed
      }
    }

    "delegate HSETNX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hsetnx(anyString, anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.hSetNx("key", "field", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hsetnx("key", "field", "value")
        succeed
      }
    }

    "delegate HINCRBY command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hincrby(anyString, anyString, anyLong)).thenReturn(mockRedisFuture)

      testClass.hIncrBy("key", "field", 1L).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hincrby("key", "field", 1L)
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

    "delegate HDEL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hdel(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.hDel("key", "field").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hdel("key", "field")
        succeed
      }
    }

    "delegate HDEL command with multiple fields to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hdel(anyString, anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.hDel("key", "field1", "field2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hdel("key", "field1", "field2")
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

    "delegate TTL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.exists(anyString, anyString)).thenReturn(mockRedisFuture)

      when(lettuceAsyncCommands.ttl(anyString)).thenReturn(mockRedisFuture)
      testClass.ttl("key").map {
        case Some(result) =>
          result mustBe (expectedValue).seconds
          verify(lettuceAsyncCommands).ttl("key")
          succeed
        case None =>
          fail(s"TTL for key should be $expectedValue second(s)")
      }
    }

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

    "delegate LRPOP command to Lettuce and lift result into a Future" in { testContext =>
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

    "delegate ZADD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zadd("key", ScoredValue.just(1, "one"))).thenReturn(mockRedisFuture)

      testClass.zAdd("key", None, ScoreWithValue(1, "one")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zadd("key", ScoredValue.just(1, "one"))
        succeed
      }
    }

    "delegate ZPOPMIN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      expectedValue.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[java.util.List[ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zpopmin("key", 1)).thenReturn(mockRedisFuture)

      testClass.zPopMin("key", 1).map { result =>
        result mustBe List(ScoreWithValue(1, "one"))
        verify(lettuceAsyncCommands).zpopmin("key", 1)
        succeed
      }
    }

    "delegate ZPOPMAX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      expectedValue.add(ScoredValue.just(10, "ten"))
      val mockRedisFuture: RedisFuture[java.util.List[ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zpopmax("key", 1)).thenReturn(mockRedisFuture)

      testClass.zPopMax("key", 1).map { result =>
        result mustBe List(ScoreWithValue(10, "ten"))
        verify(lettuceAsyncCommands).zpopmax("key", 1)
        succeed
      }
    }

    "delegate ZRANGEBYSCORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrangebyscore(meq("key"), any[io.lettuce.core.Range[java.lang.Number]]())).thenReturn(mockRedisFuture)

      testClass.zRangeByScore("key", ZRange(Double.NegativeInfinity, Double.PositiveInfinity)).map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zrangebyscore("key", io.lettuce.core.Range.create(Double.NegativeInfinity, Double.PositiveInfinity))
        succeed
      }
    }

    "delegate ZRANGEBYSCORE command with a limit to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrangebyscore(meq("key"), any[io.lettuce.core.Range[java.lang.Number]](), any[io.lettuce.core.Limit]())).thenReturn(mockRedisFuture)

      testClass.zRangeByScore("key", ZRange(Double.NegativeInfinity, Double.PositiveInfinity), Some(RangeLimit(0, 10))).map { result =>
        result mustBe List("one")
        val limitArgumentCaptor: ArgumentCaptor[io.lettuce.core.Limit] = ArgumentCaptor.forClass(classOf[io.lettuce.core.Limit]) //using captor to assert Limit offset/count as it seems equals is not implemented
        verify(lettuceAsyncCommands).zrangebyscore(meq("key"), meq(io.lettuce.core.Range.create(Double.NegativeInfinity: Number, Double.PositiveInfinity: Number)), limitArgumentCaptor.capture())
        (limitArgumentCaptor.getValue.getOffset, limitArgumentCaptor.getValue.getCount) mustBe (0, 10)
      }
    }

    "delegate ZRANGE WITHSCORES command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      expectedValue.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[java.util.List[ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrangeWithScores("key", 0, 0)).thenReturn(mockRedisFuture)

      testClass.zRangeWithScores("key", 0, 0).map { result =>
        result mustBe List(ScoreWithValue(1, "one"))
        verify(lettuceAsyncCommands).zrangeWithScores("key", 0, 0)
        succeed
      }
    }

    "delegate ZSCAN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val scoredValues: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      scoredValues.add(ScoredValue.just(1, "one"))
      val expectedValue = new io.lettuce.core.ScoredValueScanCursor[String]
      expectedValue.setCursor("1")
      expectedValue.setFinished(false)
      expectedValue.getValues.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[ScoredValueScanCursor[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.zscan(any[String], any[io.lettuce.core.ScanCursor])).thenReturn(mockRedisFuture)

      testClass.zScan("key").map { result =>
        result.cursor.cursor mustBe "1"
        result.cursor.finished mustBe false
        result.values mustBe List(ScoreWithValue(1, "one"))
        val cursorCaptor = ArgumentCaptor.forClass(classOf[io.lettuce.core.ScanCursor])
        verify(lettuceAsyncCommands).zscan(meq("key"), cursorCaptor.capture().asInstanceOf[io.lettuce.core.ScanCursor])
        val cursor = cursorCaptor.getValue.asInstanceOf[io.lettuce.core.ScanCursor]
        cursor.getCursor mustBe "0"
        cursor.isFinished mustBe false
        succeed
      }
    }

    "delegate ZSCAN command to Lettuce with match and limit args" in { testContext =>
      import testContext._
      val scoredValues: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      scoredValues.add(ScoredValue.just(1, "one"))
      val expectedValue = new io.lettuce.core.ScoredValueScanCursor[String]
      expectedValue.setCursor("1")
      expectedValue.setFinished(false)
      expectedValue.getValues.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[ScoredValueScanCursor[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.zscan(any[String], any[io.lettuce.core.ScanCursor], any[io.lettuce.core.ScanArgs])).thenReturn(mockRedisFuture)

      testClass.zScan("key", matchPattern = Some("o*"), limit = Some(10)).map { result =>
        result.cursor.cursor mustBe "1"
        result.cursor.finished mustBe false
        result.values mustBe List(ScoreWithValue(1, "one"))
        val cursorCaptor = ArgumentCaptor.forClass(classOf[io.lettuce.core.ScanCursor])
        val scanArgsCaptor = ArgumentCaptor.forClass(classOf[io.lettuce.core.ScanArgs])
        verify(lettuceAsyncCommands).zscan(meq("key"), cursorCaptor.capture(), scanArgsCaptor.capture())
        val cursor = cursorCaptor.getValue.asInstanceOf[io.lettuce.core.ScanCursor]
        cursor.getCursor mustBe "0"
        cursor.isFinished mustBe false
        succeed
      }
    }

    "delegate ZREM command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrem("key", "value")).thenReturn(mockRedisFuture)

      testClass.zRem("key", "value").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zrem("key", "value")
        succeed
      }
    }

    "delegate ZREMRANGEBYRANK command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zremrangebyrank("key", 0, 0)).thenReturn(mockRedisFuture)

      testClass.zRemRangeByRank("key", 0, 0).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zremrangebyrank("key", 0, 0)
        succeed
      }
    }

    "delegate ZREMRANGEBYSCORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zremrangebyscore(meq("key"), any[io.lettuce.core.Range[java.lang.Number]]())).thenReturn(mockRedisFuture)

      testClass.zRemRangeByScore("key", ZRange(Double.NegativeInfinity, Double.PositiveInfinity)).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zremrangebyscore("key", io.lettuce.core.Range.create(Double.NegativeInfinity, Double.PositiveInfinity))
        succeed
      }
    }

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

  class TestContext extends MockitoSugar {

    import io.lettuce.core.api.async.{RedisAsyncCommands => JRedisAsyncCommands}

    def mockRedisFutureToReturn[T](value: T): RedisFuture[T] = {
      val redisFuture = mock[RedisFuture[T]]
      val completableFuture = new CompletableFuture[T]()
      completableFuture.complete(value)
      when(redisFuture.toCompletableFuture).thenReturn(completableFuture)
      redisFuture
    }

    val redisConnection = mock[RedisConnection[String, String]]
    val lettuceAsyncCommands = mock[JRedisAsyncCommands[String, String]]

    when(redisConnection.async).thenReturn(Future.successful(lettuceAsyncCommands))

    val testClass = new LettuceRedisCommandsClient[String, String](redisConnection, cluster = false)
  }

}

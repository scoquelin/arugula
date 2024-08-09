package com.github.scoquelin.arugula

import scala.collection.immutable.ListMap

import io.lettuce.core.{KeyValue, MapScanCursor, RedisFuture}
import org.mockito.ArgumentMatchers.{any, anyLong, anyString, eq => meq}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands.ScanResults



class LettuceRedisHashAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisHashAsyncCommands" should {


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

    "delegate HEXISTS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hexists(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.hExists("key", "field").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hexists("key", "field")
        succeed
      }
    }

    "delegate HLEN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hlen(anyString)).thenReturn(mockRedisFuture)

      testClass.hLen("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hlen("key")
        succeed
      }
    }

    "delegate HKEYS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = List("field1", "field2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue.asJava)
      when(lettuceAsyncCommands.hkeys(anyString)).thenReturn(mockRedisFuture)

      testClass.hKeys("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hkeys("key")
        succeed
      }
    }

    "delegate HMGET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: Map[String, Option[String]] = Map("field1" -> Some("value1"), "field2" -> Some("value2"), "field3" -> None)
      val mockRedisFuture: RedisFuture[java.util.List[KeyValue[String, String]]] = mockRedisFutureToReturn(
        List[KeyValue[String, String]](KeyValue.fromNullable("field1", "value1"), KeyValue.fromNullable("field2", "value2"), KeyValue.fromNullable("field3", null)).asJava
      )
      when(lettuceAsyncCommands.hmget(anyString, anyString, anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.hMGet("key", "field1", "field2", "field3").map { result =>
        result mustBe ListMap("field1" -> Some("value1"), "field2" -> Some("value2"), "field3" -> None)
        verify(lettuceAsyncCommands).hmget("key", "field1", "field2", "field3")
        succeed
      }
    }

    "delegate HRANDFIELD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Some("field1")
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue.get)
      when(lettuceAsyncCommands.hrandfield(anyString)).thenReturn(mockRedisFuture)

      testClass.hRandField("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hrandfield("key")
        succeed
      }
    }

    "delegate HRANDFIELD command with count to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = List("field1", "field2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue.asJava)
      when(lettuceAsyncCommands.hrandfield(anyString, anyLong)).thenReturn(mockRedisFuture)

      testClass.hRandField("key", 2).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hrandfield("key", 2)
        succeed
      }
    }

    "delegate HRANDFIELDWITHVALUES command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Some("field1" -> "value1")
      val mockRedisFuture: RedisFuture[KeyValue[String, String]] = mockRedisFutureToReturn(KeyValue.fromNullable("field1", "value1"))
      when(lettuceAsyncCommands.hrandfieldWithvalues(anyString)).thenReturn(mockRedisFuture)

      testClass.hRandFieldWithValues("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hrandfieldWithvalues("key")
        succeed
      }
    }

    "delegate HRANDFIELDWITHVALUES command with count to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = Map("field1" -> "value1", "field2" -> "value2")
      val mockRedisFuture: RedisFuture[java.util.List[KeyValue[String, String]]] = mockRedisFutureToReturn(
        List[KeyValue[String, String]](KeyValue.fromNullable("field1", "value1"), KeyValue.fromNullable("field2", "value2")).asJava
      )
      when(lettuceAsyncCommands.hrandfieldWithvalues(anyString, anyLong)).thenReturn(mockRedisFuture)

      testClass.hRandFieldWithValues("key", 2).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hrandfieldWithvalues("key", 2)
        succeed
      }
    }

    "delegate HSCAN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = ScanResults(cursor = "0", finished = false, values = Map("field1" -> "value1", "field2" -> "value2"))
      val mapScanCursor = new MapScanCursor[String, String]()
      mapScanCursor.getMap.put("field1", "value1")
      mapScanCursor.getMap.put("field2", "value2")
      mapScanCursor.setCursor("0")
      mapScanCursor.setFinished(false)
      val mockRedisFuture: RedisFuture[MapScanCursor[String, String]] = mockRedisFutureToReturn(
        mapScanCursor
      )
      when(lettuceAsyncCommands.hscan(anyString, any[io.lettuce.core.ScanCursor])).thenReturn(mockRedisFuture)

      testClass.hScan("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hscan(meq("key"), any[io.lettuce.core.ScanCursor])
        succeed
      }
    }

    "delegate HSCAN command with cursor and match options to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = ScanResults("1", finished = true, values = Map("field3" -> "value3", "field4" -> "value4"))
      val mapScanCursor = new MapScanCursor[String, String]()
      mapScanCursor.getMap.put("field3", "value3")
      mapScanCursor.getMap.put("field4", "value4")
      mapScanCursor.setCursor("1")
      mapScanCursor.setFinished(true)
      val mockRedisFuture: RedisFuture[MapScanCursor[String, String]] = mockRedisFutureToReturn(
        mapScanCursor
      )
      when(lettuceAsyncCommands.hscan(anyString, any[io.lettuce.core.ScanCursor], any)).thenReturn(mockRedisFuture)

      testClass.hScan("key", matchPattern = Some("field*")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hscan(meq("key"), any[io.lettuce.core.ScanCursor], any[io.lettuce.core.ScanArgs])
        succeed
      }
    }

    "delegate HSTRLEN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 5L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.hstrlen(anyString, anyString)).thenReturn(mockRedisFuture)

      testClass.hStrLen("key", "field").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hstrlen("key", "field")
        succeed
      }
    }

    "delegate HVALS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = List("value1", "value2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue.asJava)
      when(lettuceAsyncCommands.hvals(anyString)).thenReturn(mockRedisFuture)

      testClass.hVals("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).hvals("key")
        succeed
      }
    }
  }

}
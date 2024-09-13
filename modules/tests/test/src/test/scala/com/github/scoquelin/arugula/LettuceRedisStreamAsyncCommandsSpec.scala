package com.github.scoquelin.arugula


import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

import com.github.scoquelin.arugula.commands.RedisStreamAsyncCommands
import io.lettuce.core.{KeyValue, RedisFuture, ScoredValue, StreamMessage}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

import java.util.concurrent.TimeUnit

class LettuceRedisStreamAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisStreamAsyncCommands" should {
    "delegate XACK command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val group = "group"
      val ids = Seq("0-0", "0-1")
      val expected = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xack(any[String], any[String], any[String], any[String]))
        .thenReturn(mockRedisFuture)
      testClass.xAck(key, group, ids:_*).map{ result =>
        result mustBe expected
        verify(lettuceAsyncCommands).xack(key, group, ids:_*)
        succeed
      }
    }


    "delegate XADD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val fields = Map("field1" -> "value1", "field2" -> "value2")
      val expected = "0-1"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xadd(any[String], any[java.util.Map[String, String]])).thenReturn(mockRedisFuture)
      testClass.xAdd(key, fields).map{ result =>
        result mustBe expected
        verify(lettuceAsyncCommands).xadd(key, fields.asJava)
        succeed
      }
    }

    "delegate XADD command with id to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val id = "0-0"
      val fields = Map("field1" -> "value1", "field2" -> "value2")
      val expected = "0-1"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xadd(any[String], any[String], any[java.util.Map[String, String]])).thenReturn(mockRedisFuture)
      testClass.xAdd(key, id, fields).map{ result =>
        result mustBe expected
        verify(lettuceAsyncCommands).xadd(key, id, fields.asJava)
        succeed
      }
    }

    "delegate XAUTOCLAIM command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val group = "group"
      val consumer = "consumer"
      val minIdleTime = 1000L
      val id = "0-0"
      val fields = Map("field1" -> "value1", "field2" -> "value2")
      val message = new StreamMessage[String, String](key, id, fields.asJava)
      val expected = new io.lettuce.core.models.stream.ClaimedMessages[String, String]("0-0", java.util.List.of(message))
      val mockRedisFuture: RedisFuture[io.lettuce.core.models.stream.ClaimedMessages[String, String]] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xautoclaim(any[String], any[io.lettuce.core.XAutoClaimArgs[String]])).thenReturn(mockRedisFuture)
      testClass.xAutoClaim(key, group, consumer, FiniteDuration(minIdleTime, TimeUnit.MILLISECONDS), id).map { result =>
        result mustBe RedisStreamAsyncCommands.ClaimedMessages(id, List(RedisStreamAsyncCommands.StreamMessage(key, id, fields)))
        verify(lettuceAsyncCommands).xautoclaim(meq(key), any[io.lettuce.core.XAutoClaimArgs[String]])
        succeed
      }
    }

    "delegate XCLAIM command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val group = "group"
      val consumer = "consumer"
      val minIdleTime = 1000L
      val ids = List("0-0", "0-1")
      val expected = java.util.List.of(new StreamMessage[String, String](key, "0-0", Map("field1" -> "value1", "field2" -> "value2").asJava))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.StreamMessage[String, String]]] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xclaim(any[String], any[io.lettuce.core.Consumer[String]], any[io.lettuce.core.XClaimArgs], any[String], any[String]))
        .thenReturn(mockRedisFuture)
      testClass.xClaim(key, group, consumer, FiniteDuration(minIdleTime, TimeUnit.MILLISECONDS), ids).map{ result =>
        result mustBe List(RedisStreamAsyncCommands.StreamMessage(key, "0-0", Map("field1" -> "value1", "field2" -> "value2")))
        verify(lettuceAsyncCommands).xclaim(meq(key), meq(io.lettuce.core.Consumer.from(group, consumer)), any[io.lettuce.core.XClaimArgs], meq(ids.head), meq(ids.last))
        succeed
      }
    }

    "delegate XDEL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val ids = Seq("0-0", "0-1")
      val expected = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xdel(any[String], any[String], any[String]))
        .thenReturn(mockRedisFuture)
      testClass.xDel(key, ids: _*).map { result =>
        result mustBe expected
        verify(lettuceAsyncCommands).xdel(key, ids: _*)
        succeed
      }
    }

    "delegate XGROUP CREATE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val group = "group"
      val expected = "OK"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xgroupCreate(any[io.lettuce.core.XReadArgs.StreamOffset[String]], any[String], any[io.lettuce.core.XGroupCreateArgs]))
        .thenReturn(mockRedisFuture)
      testClass.xGroupCreate(RedisStreamAsyncCommands.StreamOffset.latest(key), group).map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).xgroupCreate(any[io.lettuce.core.XReadArgs.StreamOffset[String]], meq(group), any[io.lettuce.core.XGroupCreateArgs])
        succeed
      }
    }

    "delegate XGROUP DELCONSUMER command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val group = "group"
      val id = "0-0"
      val expected = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xgroupDelconsumer(any[String], any[io.lettuce.core.Consumer[String]]))
        .thenReturn(mockRedisFuture)
      testClass.xGroupDelConsumer(key, group, id).map { result =>
        result mustBe expected
        verify(lettuceAsyncCommands).xgroupDelconsumer(meq(key), meq(io.lettuce.core.Consumer.from(group, id)))
        succeed
      }
    }

    "delegate XGROUP DESTROY command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val group = "group"
      val expected = true
      val mockRedisFuture: RedisFuture[java.lang.Boolean] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xgroupDestroy(any[String], any[String]))
        .thenReturn(mockRedisFuture)
      testClass.xGroupDestroy(key, group).map { result =>
        result mustBe expected
        verify(lettuceAsyncCommands).xgroupDestroy(meq(key), meq(group))
        succeed
      }
    }

    "delegate XGROUP SETID command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val group = "group"
      val id = "0-0"
      val expected = "OK"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xgroupSetid(any[io.lettuce.core.XReadArgs.StreamOffset[String]], any[String]))
        .thenReturn(mockRedisFuture)
      testClass.xGroupSetId(RedisStreamAsyncCommands.StreamOffset.latest(key), group).map { result =>
        result mustBe ()
        verify(lettuceAsyncCommands).xgroupSetid(any[io.lettuce.core.XReadArgs.StreamOffset[String]], meq(group))
        succeed
      }
    }

    "delegate XLEN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val expected = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xlen(any[String]))
        .thenReturn(mockRedisFuture)
      testClass.xLen(key).map { result =>
        result mustBe expected
        verify(lettuceAsyncCommands).xlen(meq(key))
        succeed
      }
    }

    "delegate XPENDING command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val group = "group"
      val pendingMessages = Map[String, java.lang.Long]("consumer1" -> 0L, "consumer2" -> 1L)
      val expected = new io.lettuce.core.models.stream.PendingMessages(2L, io.lettuce.core.Range.create("0-0", "0-1"), pendingMessages.asJava)
      val mockRedisFuture: RedisFuture[io.lettuce.core.models.stream.PendingMessages] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xpending(any[String], any[String]))
        .thenReturn(mockRedisFuture)
      testClass.xPending(key, group).map { result =>
        result mustBe RedisStreamAsyncCommands.PendingMessages(2L, RedisStreamAsyncCommands.XRange("0-0", "0-1"), Map("consumer1" -> 0L, "consumer2" -> 1L))
        verify(lettuceAsyncCommands).xpending(meq(key), meq(group))
        succeed
      }
    }

    "delegate XRANGE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val start = "0-0"
      val end = "0-1"
      val expected = java.util.List.of(new io.lettuce.core.StreamMessage(key, "0-0", Map("field1" -> "value1", "field2" -> "value2").asJava))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.StreamMessage[String, String]]] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xrange(any[String], any[io.lettuce.core.Range[String]]))
        .thenReturn(mockRedisFuture)
      testClass.xRange(key, RedisStreamAsyncCommands.XRange(start, end)).map { result =>
        result mustBe List(RedisStreamAsyncCommands.StreamMessage(key, "0-0", Map("field1" -> "value1", "field2" -> "value2")))
        verify(lettuceAsyncCommands).xrange(meq(key), meq(io.lettuce.core.Range.create(start, end)))
        succeed
      }
    }

    "delegate XRANGE command with args to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val start = "0-0"
      val end = "0-1"
      val count = 10L
      val expected = java.util.List.of(new io.lettuce.core.StreamMessage(key, "0-0", Map("field1" -> "value1", "field2" -> "value2").asJava))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.StreamMessage[String, String]]] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xrange(any[String], any[io.lettuce.core.Range[String]], any[io.lettuce.core.Limit])).thenReturn(mockRedisFuture)
      testClass.xRange(key, RedisStreamAsyncCommands.XRange(start, end), RedisStreamAsyncCommands.XRangeLimit(0L, 10L)).map { result =>
        result mustBe List(RedisStreamAsyncCommands.StreamMessage(key, "0-0", Map("field1" -> "value1", "field2" -> "value2")))
        verify(lettuceAsyncCommands).xrange(meq(key), meq(io.lettuce.core.Range.create(start, end)), any[io.lettuce.core.Limit])
        succeed
      }
    }

    "delegate XREAD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val streams = List(RedisStreamAsyncCommands.StreamOffset("stream1", "0-0"), RedisStreamAsyncCommands.StreamOffset("stream2", "0-1"))
      val expected: java.util.List[io.lettuce.core.StreamMessage[String, String]] = java.util.List.of(new io.lettuce.core.StreamMessage[String, String](key, "0-0", Map("field1" -> "value1", "field2" -> "value2").asJava))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.StreamMessage[String, String]]] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xread(any[io.lettuce.core.XReadArgs.StreamOffset[String]],  any[io.lettuce.core.XReadArgs.StreamOffset[String]]))
        .thenReturn(mockRedisFuture)
      testClass.xRead(streams).map { result =>
        result mustBe List(RedisStreamAsyncCommands.StreamMessage(key, "0-0", Map("field1" -> "value1", "field2" -> "value2")))
        verify(lettuceAsyncCommands).xread(any[io.lettuce.core.XReadArgs.StreamOffset[String]], any[io.lettuce.core.XReadArgs.StreamOffset[String]])
        succeed
      }
    }

    "delegate XREAD command with args to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val streams = List(RedisStreamAsyncCommands.StreamOffset("stream1", "0-0"), RedisStreamAsyncCommands.StreamOffset("stream2", "0-1"))
      val count = 10L
      val block = 1000L
      val expected: java.util.List[io.lettuce.core.StreamMessage[String, String]] = java.util.List.of(new io.lettuce.core.StreamMessage[String, String](key, "0-0", Map("field1" -> "value1", "field2" -> "value2").asJava))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.StreamMessage[String, String]]] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xread(any[io.lettuce.core.XReadArgs], any[io.lettuce.core.XReadArgs.StreamOffset[String]], any[io.lettuce.core.XReadArgs.StreamOffset[String]]))
        .thenReturn(mockRedisFuture)
      testClass.xRead(streams, count = Some(count), block = Some(FiniteDuration(block, TimeUnit.MILLISECONDS)), noAck = true).map { result =>
        result mustBe List(RedisStreamAsyncCommands.StreamMessage(key, "0-0", Map("field1" -> "value1", "field2" -> "value2")))
        verify(lettuceAsyncCommands).xread(any[io.lettuce.core.XReadArgs], any[io.lettuce.core.XReadArgs.StreamOffset[String]], any[io.lettuce.core.XReadArgs.StreamOffset[String]])
        succeed
      }
    }

    "delegate XREAD GROUP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val group = "group"
      val consumer = "consumer"
      val streams = List(RedisStreamAsyncCommands.StreamOffset("stream1", "0-0"), RedisStreamAsyncCommands.StreamOffset("stream2", "0-1"))
      val expected: java.util.List[io.lettuce.core.StreamMessage[String, String]] = java.util.List.of(new io.lettuce.core.StreamMessage[String, String]("stream-key", "0-0", Map("field1" -> "value1", "field2" -> "value2").asJava))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.StreamMessage[String, String]]] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xreadgroup(any[io.lettuce.core.Consumer[String]], any[io.lettuce.core.XReadArgs.StreamOffset[String]], any[io.lettuce.core.XReadArgs.StreamOffset[String]]))
        .thenReturn(mockRedisFuture)
      testClass.xReadGroup(group, consumer, streams).map { result =>
        result mustBe List(RedisStreamAsyncCommands.StreamMessage("stream-key", "0-0", Map("field1" -> "value1", "field2" -> "value2")))
        verify(lettuceAsyncCommands).xreadgroup(meq(io.lettuce.core.Consumer.from(group, consumer)), any[io.lettuce.core.XReadArgs.StreamOffset[String]], any[io.lettuce.core.XReadArgs.StreamOffset[String]])
        succeed
      }
    }

    "delegate XREVRANGE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val start = "0-0"
      val end = "0-1"
      val expected = java.util.List.of(new io.lettuce.core.StreamMessage(key, "0-0", Map("field1" -> "value1", "field2" -> "value2").asJava))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.StreamMessage[String, String]]] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xrevrange(any[String], any[io.lettuce.core.Range[String]]))
        .thenReturn(mockRedisFuture)
      testClass.xRevRange(key, RedisStreamAsyncCommands.XRange(start, end)).map { result =>
        result mustBe List(RedisStreamAsyncCommands.StreamMessage(key, "0-0", Map("field1" -> "value1", "field2" -> "value2")))
        verify(lettuceAsyncCommands).xrevrange(meq(key), meq(io.lettuce.core.Range.create(start, end)))
        succeed
      }
    }

    "delegate XTRIM command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val count = 10L
      val expected = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xtrim(any[String], any[java.lang.Long]))
        .thenReturn(mockRedisFuture)
      testClass.xTrim(key, count).map { result =>
        result mustBe expected
        verify(lettuceAsyncCommands).xtrim(key, count)
        succeed
      }
    }

    "delegate XTRIM with args command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val key = "stream-key"
      val expected = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expected)
      when(lettuceAsyncCommands.xtrim(any[String], any[io.lettuce.core.XTrimArgs]))
        .thenReturn(mockRedisFuture)
      testClass.xTrim(key, RedisStreamAsyncCommands.XTrimArgs(approximateTrimming = true)).map { result =>
        result mustBe expected
        verify(lettuceAsyncCommands).xtrim(meq(key), any[io.lettuce.core.XTrimArgs])
        succeed
      }
    }
  }

}

package com.github.scoquelin.arugula

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands.{RangeLimit, ScoreWithValue, ZRange}
import io.lettuce.core.{RedisFuture, ScoredValue, ScoredValueScanCursor}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

class LettuceRedisSortedSetAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisSortedSetAsyncCommands" should {
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
        result.cursor mustBe "1"
        result.finished mustBe false
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
        result.cursor mustBe "1"
        result.finished mustBe false
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

}

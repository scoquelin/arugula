package com.github.scoquelin.arugula


import scala.concurrent.duration.DurationInt

import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands
import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands.{Aggregate, RangeLimit, ScoreWithValue, SortOrder, ZAddOptions, AggregationArgs, ZRange}
import io.lettuce.core.{KeyValue, RedisFuture, ScoredValue, ScoredValueScanCursor}
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

    "delegate BZMPOP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: KeyValue[String, ScoredValue[String]] = KeyValue.just("key", ScoredValue.just(1.0, "one"))
      val mockRedisFuture: RedisFuture[KeyValue[String, ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.bzmpop(any[Double],  any[io.lettuce.core.ZPopArgs], any[String])).thenReturn(mockRedisFuture)

      testClass.bzMPop(0.milliseconds,SortOrder.Min, "key").map { result =>
        result mustBe Some(RedisSortedSetAsyncCommands.ScoreWithKeyValue(1.0, "key", "one"))
        verify(lettuceAsyncCommands).bzmpop(meq(0.0), any[io.lettuce.core.ZPopArgs], meq("key"))
        succeed
      }
    }

    "delegate BZMPOP command with count to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val scoredValues = new java.util.ArrayList[ScoredValue[String]]
      scoredValues.add(ScoredValue.just(1.0, "one"))
      val expectedValue: KeyValue[String, java.util.List[ScoredValue[String]]] = KeyValue.just("key", scoredValues)
      val mockRedisFuture: RedisFuture[KeyValue[String, java.util.List[ScoredValue[String]]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.bzmpop(any[Double], any[Int],  any[io.lettuce.core.ZPopArgs], any[String])).thenReturn(mockRedisFuture)

      testClass.bzMPop(0.milliseconds, 1, SortOrder.Min, "key").map { result =>
        result mustBe List(RedisSortedSetAsyncCommands.ScoreWithKeyValue(1.0, "key", "one"))
        verify(lettuceAsyncCommands).bzmpop(meq(0.0), meq(1), any[io.lettuce.core.ZPopArgs], meq("key"))
        succeed
      }
    }

    "delegate BZPOPMIN command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: KeyValue[String,ScoredValue[String]] = KeyValue.just("key", ScoredValue.just(1.0, "one"))
      val mockRedisFuture: RedisFuture[KeyValue[String, ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.bzpopmin(0.0, "key")).thenReturn(mockRedisFuture)

      testClass.bzPopMin(0.milliseconds, "key").map { result =>
        result mustBe Some(RedisSortedSetAsyncCommands.ScoreWithKeyValue(1.0, "key", "one"))
        verify(lettuceAsyncCommands).bzpopmin(0.0, "key")
        succeed
      }
    }

    "delegate ZADD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zadd("key", ScoredValue.just(1, "one"))).thenReturn(mockRedisFuture)

      testClass.zAdd("key", ScoreWithValue(1, "one")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zadd("key", ScoredValue.just(1, "one"))
        succeed
      }
    }

    "delegate ZADD command with multiple values to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zadd("key", ScoredValue.just(1, "one"), ScoredValue.just(2, "two"))).thenReturn(mockRedisFuture)

      testClass.zAdd("key", ScoreWithValue(1, "one"), ScoreWithValue(2, "two")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zadd("key", ScoredValue.just(1, "one"), ScoredValue.just(2, "two"))
        succeed
      }
    }

    "delegate ZADD command with NX option to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zadd(any[String], any[io.lettuce.core.ZAddArgs], any[ScoredValue[String]])).thenReturn(mockRedisFuture)

      testClass.zAdd("key", ZAddOptions.NX, ScoreWithValue(1, "one")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zadd(meq("key"), any[io.lettuce.core.ZAddArgs], meq(ScoredValue.just(1, "one")))
        succeed
      }
    }

    "delegate ZADDINCR command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1.0
      val mockRedisFuture: RedisFuture[java.lang.Double] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zaddincr("key", 1.0, "one")).thenReturn(mockRedisFuture)

      testClass.zAddIncr("key", 1.0, "one").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).zaddincr("key", 1.0, "one")
        succeed
      }
    }

    "delegate ZADDINCR command with args to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2.0
      val mockRedisFuture: RedisFuture[java.lang.Double] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zaddincr(any[String], any[io.lettuce.core.ZAddArgs], any[Double], any[String])).thenReturn(mockRedisFuture)

      testClass.zAddIncr("key", ZAddOptions.NX, 1.0, "one").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).zaddincr(meq("key"), any[io.lettuce.core.ZAddArgs], meq(1.0), meq("one"))
        succeed
      }
    }

    "delegate ZCARD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zcard("key")).thenReturn(mockRedisFuture)

      testClass.zCard("key").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zcard("key")
        succeed
      }
    }

    "delegate ZCOUNT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zcount("key", io.lettuce.core.Range.create(0, 1))).thenReturn(mockRedisFuture)

      testClass.zCount("key", ZRange(0, 1)).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zcount("key", io.lettuce.core.Range.create(0, 1))
        succeed
      }
    }

    "delegate ZDIFF command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zdiff("key1", "key2")).thenReturn(mockRedisFuture)

      testClass.zDiff("key1", "key2").map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zdiff("key1", "key2")
        succeed
      }
    }

    "delegate ZDIFFSTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zdiffstore("destination", "key1", "key2")).thenReturn(mockRedisFuture)

      testClass.zDiffStore("destination", "key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zdiffstore("destination", "key1", "key2")
        succeed
      }
    }

    "delegate ZDIFFWITHSCORES command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      expectedValue.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[java.util.List[ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zdiffWithScores("key1", "key2")).thenReturn(mockRedisFuture)

      testClass.zDiffWithScores("key1", "key2").map { result =>
        result mustBe List(ScoreWithValue(1, "one"))
        verify(lettuceAsyncCommands).zdiffWithScores("key1", "key2")
        succeed
      }
    }

    "delegate ZINTER command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zinter("key1", "key2")).thenReturn(mockRedisFuture)

      testClass.zInter("key1", "key2").map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zinter("key1", "key2")
        succeed
      }
    }

    "delegate ZINTER command with weights to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zinter(any[io.lettuce.core.ZStoreArgs], any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.zInter(AggregationArgs(weights = Seq(1.0, 2.0)), "key1", "key2").map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zinter(any[io.lettuce.core.ZAggregateArgs], meq("key1"), meq("key2"))
        succeed
      }
    }

    "delegate ZINTER command with weights and AGGREGATE MIN to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zinter(any[io.lettuce.core.ZStoreArgs], any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.zInter(AggregationArgs(Aggregate.Min, weights = Seq(1.0, 2.0)), "key1", "key2").map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zinter(any[io.lettuce.core.ZAggregateArgs], meq("key1"), meq("key2"))
        succeed
      }
    }

    "delegate ZINTERCARD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zintercard("key1", "key2")).thenReturn(mockRedisFuture)

      testClass.zInterCard("key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zintercard("key1", "key2")
        succeed
      }
    }

    "delegate ZINTERSTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zinterstore("destination", "key1", "key2")).thenReturn(mockRedisFuture)

      testClass.zInterStore("destination", "key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zinterstore("destination", "key1", "key2")
        succeed
      }
    }

    "delegate ZINTERSTORE command with weights to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zinterstore(any[String], any[io.lettuce.core.ZStoreArgs], any[String], any[String])).thenReturn(mockRedisFuture)

      testClass.zInterStore("destination", AggregationArgs(weights = Seq(1.0, 2.0)), "key1", "key2").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zinterstore(meq("destination"), any[io.lettuce.core.ZStoreArgs], meq("key1"), meq("key2"))
        succeed
      }
    }

    "delegate ZINTERWITHSCORES command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      expectedValue.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[java.util.List[ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zinterWithScores("key1", "key2")).thenReturn(mockRedisFuture)

      testClass.zInterWithScores("key1", "key2").map { result =>
        result mustBe List(ScoreWithValue(1, "one"))
        verify(lettuceAsyncCommands).zinterWithScores("key1", "key2")
        succeed
      }
    }

    "delegate ZLEXCOUNT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zlexcount("key", io.lettuce.core.Range.create("b", "f"))).thenReturn(mockRedisFuture)

      testClass.zLexCount("key", ZRange("b", "f")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zlexcount("key", io.lettuce.core.Range.create("b", "f"))
        succeed
      }
    }

    "delegate ZMPOP command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: KeyValue[String, ScoredValue[String]] = KeyValue.just("key", ScoredValue.just(1.0, "one"))
      val mockRedisFuture: RedisFuture[KeyValue[String, ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zmpop(any[io.lettuce.core.ZPopArgs], any[String])).thenReturn(mockRedisFuture)

      testClass.zMPop(SortOrder.Min, "key").map { result =>
        result mustBe Some(RedisSortedSetAsyncCommands.ScoreWithKeyValue(1.0, "key", "one"))
        verify(lettuceAsyncCommands).zmpop(any[io.lettuce.core.ZPopArgs], meq("key"))
        succeed
      }
    }

    "delegate ZMPOP command with count to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val scoredValues = new java.util.ArrayList[ScoredValue[String]]
      scoredValues.add(ScoredValue.just(1.0, "one"))
      val expectedValue: KeyValue[String, java.util.List[ScoredValue[String]]] = KeyValue.just("key", scoredValues)
      val mockRedisFuture: RedisFuture[KeyValue[String, java.util.List[ScoredValue[String]]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zmpop(any[Int], any[io.lettuce.core.ZPopArgs], any[String])).thenReturn(mockRedisFuture)

      testClass.zMPop(1, SortOrder.Min, "key").map { result =>
        result mustBe List(RedisSortedSetAsyncCommands.ScoreWithKeyValue(1.0, "key", "one"))
        verify(lettuceAsyncCommands).zmpop(meq(1), any[io.lettuce.core.ZPopArgs], meq("key"))
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

    "delegate ZRANGEBYLEX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrangebylex("key", io.lettuce.core.Range.create("b", "f"), io.lettuce.core.Limit.unlimited())).thenReturn(mockRedisFuture)

      testClass.zRangeByLex("key", ZRange("b", "f"), None).map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zrangebylex("key", io.lettuce.core.Range.create("b", "f"), io.lettuce.core.Limit.unlimited())
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

    "delegate ZRANGE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrange("key", 0, 0)).thenReturn(mockRedisFuture)

      testClass.zRange("key", 0, 0).map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zrange("key", 0, 0)
        succeed
      }
    }

    "delegate ZRANGESTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrangestore(meq("destination"), meq("key"), any[io.lettuce.core.Range[java.lang.Long]])).thenReturn(mockRedisFuture)

      testClass.zRangeStore("destination", "key", 0, 1).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zrangestore("destination", "key", io.lettuce.core.Range.create(0L, 1L))
        succeed
      }
    }

    "delegate ZRANGESTOREBYLEX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrangestorebylex(meq("destination"), meq("key"), any[io.lettuce.core.Range[String]], any[io.lettuce.core.Limit])).thenReturn(mockRedisFuture)

      testClass.zRangeStoreByLex("destination", "key", ZRange("b", "f")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zrangestorebylex("destination", "key", io.lettuce.core.Range.create("b", "f"), io.lettuce.core.Limit.unlimited())
        succeed
      }
    }

    "delegate ZRANGESTOREBYSCORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrangestorebyscore(meq("destination"), meq("key"), any[io.lettuce.core.Range[java.lang.Number]], any[io.lettuce.core.Limit])).thenReturn(mockRedisFuture)

      testClass.zRangeStoreByScore("destination", "key", ZRange(Double.NegativeInfinity, Double.PositiveInfinity)).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zrangestorebyscore("destination", "key", io.lettuce.core.Range.create(Double.NegativeInfinity, Double.PositiveInfinity), io.lettuce.core.Limit.unlimited())
        succeed
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

    "delegate ZINCRBY command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 2.0
      val mockRedisFuture: RedisFuture[java.lang.Double] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zincrby("key", 1.0, "one")).thenReturn(mockRedisFuture)

      testClass.zIncrBy("key", 1.0, "one").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zincrby("key", 1.0, "one")
        succeed
      }
    }

    "delegate ZRANK command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 0L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrank("key", "one")).thenReturn(mockRedisFuture)

      testClass.zRank("key", "one").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).zrank("key", "one")
        succeed
      }
    }

    "delegate ZRANK WITHSCORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = ScoreWithValue(0.0, 1L)
      val mockRedisFuture: RedisFuture[ScoredValue[java.lang.Long]] = mockRedisFutureToReturn(ScoredValue.just(0.0, java.lang.Long.valueOf(1)))

      when(lettuceAsyncCommands.zrankWithScore("key", "one")).thenReturn(mockRedisFuture)

      testClass.zRankWithScore("key", "one").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).zrankWithScore("key", "one")
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

    "delegate ZSCORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1.0
      val mockRedisFuture: RedisFuture[java.lang.Double] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zscore("key", "one")).thenReturn(mockRedisFuture)

      testClass.zScore("key", "one").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).zscore("key", "one")
        succeed
      }
    }

    "delegate ZMSCORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = new java.util.ArrayList[java.lang.Double]
      expectedValue.add(0, 1.0)
      val mockRedisFuture: RedisFuture[java.util.List[java.lang.Double]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zmscore("key", "one")).thenReturn(mockRedisFuture)

      testClass.zMScore("key", "one").map { result =>
        result mustBe List(Some(1.0))
        verify(lettuceAsyncCommands).zmscore("key", "one")
        succeed
      }
    }

    "delegate ZREVRANGE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrange("key", 0, 0)).thenReturn(mockRedisFuture)

      testClass.zRevRange("key", 0, 0).map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zrevrange("key", 0, 0)
        succeed
      }
    }

    "delegate ZREVRANGESTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrangestore(meq("destination"), meq("key"), any[io.lettuce.core.Range[java.lang.Long]]())).thenReturn(mockRedisFuture)

      testClass.zRevRangeStore("destination", "key", 0, 1).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zrevrangestore("destination", "key", io.lettuce.core.Range.create(0L, 1L))
        succeed
      }
    }

    "delegate ZREVRANGESTOREBYLEX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrangestorebylex(meq("destination"), meq("key"), any[io.lettuce.core.Range[String]], any[io.lettuce.core.Limit])).thenReturn(mockRedisFuture)

      testClass.zRevRangeStoreByLex("destination", "key", ZRange("b", "f")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zrevrangestorebylex("destination", "key", io.lettuce.core.Range.create("b", "f"), io.lettuce.core.Limit.unlimited())
        succeed
      }
    }

    "delegate ZREVRANGESTOREBYSCORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrangestorebyscore(meq("destination"), meq("key"), any[io.lettuce.core.Range[java.lang.Number]], any[io.lettuce.core.Limit])).thenReturn(mockRedisFuture)

      testClass.zRevRangeStoreByScore("destination", "key", ZRange(Double.NegativeInfinity, Double.PositiveInfinity)).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zrevrangestorebyscore("destination", "key", io.lettuce.core.Range.create(Double.NegativeInfinity, Double.PositiveInfinity), io.lettuce.core.Limit.unlimited())
        succeed
      }
    }

    "delegate ZREVRANGEBYLEX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrangebylex("key", io.lettuce.core.Range.create("b", "f"), io.lettuce.core.Limit.unlimited())).thenReturn(mockRedisFuture)

      testClass.zRevRangeByLex("key", ZRange("b", "f"), None).map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zrevrangebylex("key", io.lettuce.core.Range.create("b", "f"), io.lettuce.core.Limit.unlimited())
        succeed
      }
    }

    "delegate ZREVRANGE WITHSCORES command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      expectedValue.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[java.util.List[ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrangeWithScores("key", 0, 0)).thenReturn(mockRedisFuture)

      testClass.zRevRangeWithScores("key", 0, 0).map { result =>
        result mustBe List(ScoreWithValue(1, "one"))
        verify(lettuceAsyncCommands).zrevrangeWithScores("key", 0, 0)
        succeed
      }
    }

    "delegate ZREVRANGEBYSCORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrangebyscore(meq("key"), any[io.lettuce.core.Range[java.lang.Number]]())).thenReturn(mockRedisFuture)

      testClass.zRevRangeByScore("key", ZRange(Double.NegativeInfinity, Double.PositiveInfinity)).map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zrevrangebyscore("key", io.lettuce.core.Range.create(Double.NegativeInfinity, Double.PositiveInfinity))
        succeed
      }
    }

    "delegate ZREVRANGEBYSCORE command with a limit to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrangebyscore(meq("key"), any[io.lettuce.core.Range[java.lang.Number]](), any[io.lettuce.core.Limit]())).thenReturn(mockRedisFuture)

      testClass.zRevRangeByScore("key", ZRange(Double.NegativeInfinity, Double.PositiveInfinity), Some(RangeLimit(0, 10))).map { result =>
        result mustBe List("one")
        val limitArgumentCaptor: ArgumentCaptor[io.lettuce.core.Limit] = ArgumentCaptor.forClass(classOf[io.lettuce.core.Limit]) //using captor to assert Limit offset/count as it seems equals is not implemented
        verify(lettuceAsyncCommands).zrevrangebyscore(meq("key"), meq(io.lettuce.core.Range.create(Double.NegativeInfinity: Number, Double.PositiveInfinity: Number)), limitArgumentCaptor.capture())
        (limitArgumentCaptor.getValue.getOffset, limitArgumentCaptor.getValue.getCount) mustBe (0, 10)
      }
    }

    "delegate ZREVRANGEBYSCORE WITHSCORES command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      expectedValue.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[java.util.List[ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrangebyscoreWithScores("key", io.lettuce.core.Range.create(0, 1))).thenReturn(mockRedisFuture)

      testClass.zRevRangeByScoreWithScores("key", ZRange(0, 1)).map { result =>
        result mustBe List(ScoreWithValue(1, "one"))
        verify(lettuceAsyncCommands).zrevrangebyscoreWithScores("key", io.lettuce.core.Range.create(0, 1))
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

    "delegate ZRANGEBYSCORE WITHSCORES command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      expectedValue.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[java.util.List[ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrangebyscoreWithScores(meq("key"), any[io.lettuce.core.Range[java.lang.Number]]())).thenReturn(mockRedisFuture)

      testClass.zRangeByScoreWithScores("key", ZRange(Double.NegativeInfinity, Double.PositiveInfinity)).map { result =>
        result mustBe List(ScoreWithValue(1, "one"))
        verify(lettuceAsyncCommands).zrangebyscoreWithScores("key", io.lettuce.core.Range.create(Double.NegativeInfinity, Double.PositiveInfinity))
        succeed
      }
    }

    "delegate ZREVRANK command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 0L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrevrank("key", "one")).thenReturn(mockRedisFuture)

      testClass.zRevRank("key", "one").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).zrevrank("key", "one")
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

    "delegate ZREMRANGEBYLEX command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zremrangebylex("key", io.lettuce.core.Range.create("b", "f"))).thenReturn(mockRedisFuture)

      testClass.zRemRangeByLex("key", ZRange("b", "f")).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).zremrangebylex("key", io.lettuce.core.Range.create("b", "f"))
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

    "delegate ZRANDMEMBER command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "one"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrandmember("key")).thenReturn(mockRedisFuture)

      testClass.zRandMember("key").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).zrandmember("key")
        succeed
      }
    }

    "delegate ZRANDMEMBER command with count to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[String] = new java.util.ArrayList[String]
      expectedValue.add(0, "one")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrandmember("key", 1)).thenReturn(mockRedisFuture)

      testClass.zRandMember("key", 1).map { result =>
        result mustBe List("one")
        verify(lettuceAsyncCommands).zrandmember("key", 1)
        succeed
      }
    }

    "delegate ZRANDMEMBER WITHSCORES command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = ScoredValue.just(1, "one")
      val mockRedisFuture: RedisFuture[ScoredValue[String]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrandmemberWithScores("key")).thenReturn(mockRedisFuture)

      testClass.zRandMemberWithScores("key").map { result =>
        result mustBe Some(ScoreWithValue(1, "one"))
        verify(lettuceAsyncCommands).zrandmemberWithScores("key")
        succeed
      }
    }

    "delegate ZRANDMEMBER WITHSCORES command with count to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.List[ScoredValue[String]] = new java.util.ArrayList[ScoredValue[String]]
      expectedValue.add(ScoredValue.just(1, "one"))
      val mockRedisFuture: RedisFuture[java.util.List[ScoredValue[String]]] = mockRedisFutureToReturn(expectedValue)

      when(lettuceAsyncCommands.zrandmemberWithScores("key", 1)).thenReturn(mockRedisFuture)

      testClass.zRandMemberWithScores("key", 1).map { result =>
        result mustBe List(ScoreWithValue(1, "one"))
        verify(lettuceAsyncCommands).zrandmemberWithScores("key", 1)
        succeed
      }
    }
  }
}

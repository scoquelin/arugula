package com.github.scoquelin.arugula

import scala.collection.immutable.ListMap
import scala.concurrent.Future

import com.github.scoquelin.arugula.codec.RedisCodec
import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands.{Aggregate, AggregationArgs, RangeLimit, ScoreWithKeyValue, ScoreWithValue, SortOrder, ZAddOptions, ZRange}
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands.InitialCursor
import com.github.scoquelin.arugula.commands.RedisListAsyncCommands
import com.github.scoquelin.arugula.commands.RedisStringAsyncCommands.{BitFieldCommand, BitFieldDataType}

import java.util.concurrent.TimeUnit

class RedisCommandsIntegrationSpec extends BaseRedisCommandsIntegrationSpec with Matchers {
  import RedisCommandsIntegrationSpec.randomKey

  "RedisCommandsClient" when {

    "leveraging RedisBaseAsyncCommands" should {

      "ping" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          for {
            response <- client.ping
            _ <- response shouldBe "PONG"
          } yield succeed
        }
      }
    }

    "leveraging RedisStringAsyncCommands" should {

      "create, check, retrieve, and delete a key holding a Long value" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsLongCodec) { client =>
          val key = randomKey()
          val value = 1L

          for {
            _ <- client.set(key, value)
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe true
            existingKeyAdded <- client.setNx(key, value) //noop since key already exists
            _ <- existingKeyAdded shouldBe false
            newKeyAdded <- client.setNx("newKey", value)
            _ <- newKeyAdded shouldBe true
            keyValue <- client.get(key)
            _ <- keyValue match {
              case Some(expectedValue) => expectedValue shouldBe value
              case None => fail("Expected value not found")
            }
            deleted <- client.del(key)
            _ <- deleted shouldBe 1L
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe false
            valuePriorToSet <- client.getSet(key, value)
            _ <- valuePriorToSet match {
              case Some(_) => fail("Expected value not found")
              case None => succeed
            }
            priorValue <- client.getDel(key)
            _ <- priorValue match {
              case Some(expectedValue) => expectedValue shouldBe value
              case None => fail("Expected value not found")
            }
          } yield succeed
        }
      }

      "create, check, retrieve, and delete a key holding a String value" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey()
          val value = "value"

          for {
            _ <- client.set(key, value)
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe true
            existingKeyAdded <- client.setNx(key, value) //noop since key already exists
            _ <- existingKeyAdded shouldBe false
            newKeyAdded <- client.setNx("newKey", value)
            _ <- newKeyAdded shouldBe true
            keyValue <- client.get(key)
            _ <- keyValue match {
              case Some(expectedValue) => expectedValue shouldBe value
              case None => fail("Expected value not found")
            }
            deleted <- client.del(key)
            _ <- deleted shouldBe 1L
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe false
            valuePriorToSet <- client.getSet(key, value)
            _ <- valuePriorToSet match {
              case Some(_) => fail("Expected value not found")
              case None => succeed
            }
            priorValue <- client.getDel(key)
            _ <- priorValue match {
              case Some(expectedValue) => expectedValue shouldBe value
              case None => fail("Expected value not found")
            }
          } yield succeed
        }
      }

      "increment and decrement a key holding a Long value" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsLongCodec) { client =>
          val key = randomKey("increment-key")
          for {
            _ <- client.set(key, 0L)
            _ <- client.incr(key)
            value <- client.get(key)
            _ <- value match {
              case Some(expectedValue) => expectedValue shouldBe 1
              case None => fail("Expected value not found")
            }
            _ <- client.incrBy(key, 5)
            value <- client.get(key)
            _ <- value match {
              case Some(expectedValue) => expectedValue shouldBe 6
              case None => fail("Expected value not found")
            }
            _ <- client.decr(key)

            value <- client.get(key)
            _ <- value match {
              case Some(expectedValue) => expectedValue shouldBe 5
              case None => fail("Expected value not found")
            }

            _ <- client.decrBy(key, 3)

            value <- client.get(key)

            _ <- value match {
              case Some(expectedValue) => expectedValue shouldBe 2
              case None => fail("Expected value not found")
            }

          } yield succeed
        }
      }

      "increment and decrement a key holding a String value" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("increment-key")
          for {
            _ <- client.incr(key)
            value <- client.get(key)
            _ <- value match {
              case Some(expectedValue) => expectedValue shouldBe "1"
              case None => fail("Expected value not found")
            }
            _ <- client.incrBy(key, 5)
            value <- client.get(key)
            _ <- value match {
              case Some(expectedValue) => expectedValue shouldBe "6"
              case None => fail("Expected value not found")
            }
            _ <- client.decr(key)

            value <- client.get(key)
            _ <- value match {
              case Some(expectedValue) => expectedValue shouldBe "5"
              case None => fail("Expected value not found")
            }

            _ <- client.decrBy(key, 3)

            value <- client.get(key)

            _ <- value match {
              case Some(expectedValue) => expectedValue shouldBe "2"
              case None => fail("Expected value not found")
            }

            _ <- client.incrByFloat(key, 0.5)

            value <- client.get(key)

            _ <- value match {
              case Some(expectedValue) => expectedValue shouldBe "2.5"
              case None => fail("Expected value not found")
            }

          } yield succeed
        }
      }

      "create, check, retrieve, and delete a key with expiration" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("expiring-key")
          val value = "value"
          val expireIn = 30.minutes

          for {
            _ <- client.setEx(key, value, expireIn)
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe true
            keyValue <- client.get(key)
            _ <- keyValue match {
              case Some(expectedValue) => expectedValue shouldBe value
              case None => fail("Expected value not found")
            }
            ttl <- client.ttl(key)
            _ <- ttl match {
              case Some(timeToLive) => assert(timeToLive > (expireIn - 1.minute) && timeToLive <= expireIn)
              case None => fail("Expected time to live not found")
            }
            longDuration = FiniteDuration(3, TimeUnit.DAYS)
            getExp <- client.getEx(key, longDuration)
            _ <- getExp match {
              case Some(expectedValue) => expectedValue shouldBe value
              case None => fail("Expected value not found")
            }
            getTtl <- client.ttl(key)
            _ <- getTtl match {
              case Some(timeToLive) => assert(timeToLive > (longDuration - 1.minute) && timeToLive <= longDuration)
              case None => fail("Expected time to live not found")
            }
            deleted <- client.del(key)
            _ <- deleted shouldBe 1L
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe false
          } yield succeed
        }
      }

      "support string range operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("range-key")
          for {
            lenResult <- client.append(key, "Hello")
            _ = lenResult shouldBe 5L
            lenResult <- client.append(key, ", World!")
            _ = lenResult shouldBe 13L
            range <- client.getRange(key, 0, 4)
            _ = range shouldBe Some("Hello")
            range <- client.getRange(key, -6, -1)
            _ = range shouldBe Some("World!")
            _ = client.setRange(key, 7, "Redis")
            updatedValue <- client.get(key)
            _ = updatedValue shouldBe Some("Hello, Redis!")
            strLen <- client.strLen(key)
            _ = strLen shouldBe 13L
          } yield succeed

        }
      }

      "support bit operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val suffix = "{user1}"
          val key1 = randomKey("bit-key1") + suffix
          val key2 = randomKey("bit-key2") + suffix
          for {
            bitSet <- client.setBit(key1, 0, 1)
            _ <- bitSet shouldBe 0L
            bitSet <- client.setBit(key1, 0, 0)
            _ <- bitSet shouldBe 1L
            bitSet <- client.setBit(key1, 0, 1)
            _ <- bitSet shouldBe 0L
            bitSet <- client.setBit(key1, 0, 1)
            _ <- bitSet shouldBe 1L
            bitGet <- client.getBit(key1, 0)
            _ <- bitGet shouldBe 1L
            bitGet <- client.getBit(key1, 1)
            _ <- bitGet shouldBe 0L
            bitCount <- client.bitCount(key1)
            _ <- bitCount shouldBe 1L
            bitFieldResult <- client.bitField(key1, Seq(
              BitFieldCommand.set(BitFieldDataType.Unsigned(8), 1),
              BitFieldCommand.get(BitFieldDataType.Unsigned(8), 1),
              BitFieldCommand.incrBy(BitFieldDataType.Unsigned(8), 1, 1),
              BitFieldCommand.get(BitFieldDataType.Unsigned(8), 1),
            ))
            _ <- bitFieldResult shouldBe Seq(128, 2, 3, 3)
            _ <- client.setBit(key2, 0, 1)
            bitOpAnd <- client.bitOpAnd(key1, key2)
            _ <- bitOpAnd shouldBe 1L
            bitOpOr <- client.bitOpOr(key1, key2)
            _ <- bitOpOr shouldBe 1L
            bitOpXor <- client.bitOpXor(key1, key2)
            _ <- bitOpXor shouldBe 1L
            bitOpNot <- client.bitOpNot(key1, key2)
            _ <- bitOpNot shouldBe 1L
            bitGet <- client.getBit(key1, 0)
            _ <- bitGet shouldBe 0L
            posBit <- client.bitPos(key1, state = true)
            _ <- posBit shouldBe 1L
            posBit <- client.bitPos(key1, state = false)
            _ <- posBit shouldBe 0L
          } yield succeed
        }
      }

      "support multiple key operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val suffix = "{user1}"
          val key1 = randomKey("k1") + suffix
          val key2 = randomKey("k2") + suffix
          val key3 = randomKey("k3") + suffix
          val key4 = randomKey("k4") + suffix
          for {
            _ <- client.mSet(Map(key1 -> "value1", key2 -> "value2", key3 -> "value3"))
            values <- client.mGet(key1, key2, key3, key4)
            _ <- values shouldBe ListMap(key1 -> Some("value1"), key2 -> Some("value2"), key3 -> Some("value3"), key4 -> None)
            nxResult <- client.mSetNx(Map(key4 -> "value4"))
            _ = nxResult shouldBe true
            values <- client.mGet(key1, key2, key3, key4)
            _ = values shouldBe ListMap(key1 -> Some("value1"), key2 -> Some("value2"), key3 -> Some("value3"), key4 -> Some("value4"))
          } yield succeed

        }
      }

    }

    "leveraging RedisListAsyncCommands" should {

      "create, retrieve, and delete values in a list" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("list-key")
          val values = List("one", "two", "three")

          for {
            _ <- client.lPush(key, values: _*)
            range <- client.lRange(key, 0, -1)
            count <- client.lLen(key)
            pos <- client.lPos(key, "two")
            _ <- count shouldBe values.size
            _ <- range shouldBe values.reverse
            _ <- pos shouldBe Some(1)
            popped <- client.lPop(key)
            _ <- popped shouldBe Some("three")
            popped <- client.lPop(key)
            _ <- popped shouldBe Some("two")
            popped <- client.lPop(key)
            _ <- popped shouldBe Some("one")
            popped <- client.lPop(key)
            _ <- popped shouldBe None
            _ <- client.rPush(key, values: _*)
            range <- client.lRange(key, 0, -1)
            _ = range shouldBe values
            _ <- client.lRem(key, 1, "two")
            range <- client.lRange(key, 0, -1)
            _ <- range shouldBe List("one", "three")
            _ <- client.lTrim(key, 0, 0)
            range <- client.lRange(key, 0, -1)
            _ <- range shouldBe List("one")
            index <- client.lIndex(key, 0)
            _ <- index shouldBe Some("one")
            index <- client.lIndex(key, 1)
            _ <- index shouldBe None
            popped <- client.rPop(key)
            _ <- popped shouldBe Some("one")
            endState <- client.lRange(key, 0, -1)
            _ <- endState.isEmpty shouldBe true
            _ <- client.lPush(key, "one", "two", "three")
            _ <- client.lInsert(key, before = true, "two", "1.5")
            range <- client.lRange(key, 0, -1)
            _ <- range shouldBe List("three", "1.5", "two", "one")
            lPushXResult <- client.lPushX(key, "zero")
            _ <- lPushXResult shouldBe 5L
            lSetResult <- client.lSet(key, 1, "1.75")
            _ <- lSetResult shouldBe ()
            rPushXResult <- client.rPushX(key, "four")
            _ <- rPushXResult shouldBe 6L
          } yield succeed
        }
      }

      "support move operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val suffix = "{user1}"
          val key1 = randomKey("list-key1") + suffix
          val key2 = randomKey("list-key2") + suffix
          val key3 = randomKey("list-key3") + suffix

          for {
            _ <- client.lPush(key1, "one", "two", "three")
            _ <- client.lPush(key2, "four", "five", "six")
            _ <- client.lMove(key1, key2, RedisListAsyncCommands.Side.Left, RedisListAsyncCommands.Side.Right)
            _ <- client.blMove(key1, key3, RedisListAsyncCommands.Side.Left, RedisListAsyncCommands.Side.Right, timeout = FiniteDuration(100, TimeUnit.MILLISECONDS))
            key1Range <- client.lRange(key1, 0, -1)
            _ <- key1Range shouldBe List("one")
            key2Range <- client.lRange(key2, 0, -1)
            _ <- key2Range shouldBe List("six", "five", "four", "three")
            key3Range <- client.lRange(key3, 0, -1)
            _ <- key3Range shouldBe List("two")

          } yield succeed
        }
      }

      "support pop operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val suffix = "{user1}"
          val key1 = randomKey("list-key1") + suffix
          val destKey = randomKey("list-key2") + suffix
          for {
            _ <- client.lPush(key1, "one", "two", "three")
            popResult <- client.lPop(key1)
            _ <- popResult shouldBe Some("three")
            key1Range <- client.lRange(key1, 0, -1)
            _ <- key1Range shouldBe List("two", "one")
            blPopResult <- client.blPop(timeout = FiniteDuration(1, TimeUnit.MILLISECONDS), key1)
            _ <- blPopResult shouldBe Some((key1, "two"))
            key1Range <- client.lRange(key1, 0, -1)
            _ <- key1Range shouldBe List("one")
            rPopResult <- client.rPop(key1)
            _ <- rPopResult shouldBe Some("one")
            _ <- client.rPush(key1, "one")
            brPopResult <- client.brPop(timeout = FiniteDuration(1, TimeUnit.MILLISECONDS), key1)
            _ <- brPopResult shouldBe Some((key1, "one"))
            _ <- client.rPush(key1, "one")
            brPopLPushResult <- client.brPopLPush(timeout = FiniteDuration(1, TimeUnit.MILLISECONDS), key1, destKey)
            _ <- brPopLPushResult shouldBe Some("one")
            _ <- client.rPush(key1, "one")
            rPopLPushResult <- client.rPopLPush(key1, destKey)
            _ <- rPopLPushResult shouldBe Some("one")
          } yield succeed
        }
      }

      "support multi pop operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val suffix = "{user1}"
          val key1 = randomKey("list-key1") + suffix
          val key2 = randomKey("list-key2") + suffix
          for {
            _ <- client.lPush(key1, "one", "two", "three")
            _ <- client.lPush(key2, "four", "five", "six")
            mPopResult <- client.lMPop(List(key1, key2), count = 2)
            _ <- mPopResult shouldBe Some((key1, List("three", "two")))
            key1Range <- client.lRange(key1, 0, -1)
            _ <- key1Range shouldBe List("one")
            key2Range <- client.lRange(key2, 0, -1)
            _ <- key2Range shouldBe List("six", "five", "four")
            blPopResult <- client.blMPop(List(key1, key2), count = 2, timeout = FiniteDuration(1, TimeUnit.MILLISECONDS))
            _ <- blPopResult shouldBe Some((key1, List("one")))
            key1Range <- client.lRange(key1, 0, -1)
            _ <- key1Range shouldBe List()
            key2Range <- client.lRange(key2, 0, -1)
            _ <- key2Range shouldBe List("six", "five", "four")
          } yield succeed
        }
      }
    }

    "leveraging RedisSortedSetAsyncCommands" should {

      "create, retrieve, scan and delete values in a sorted set" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("sorted-set")

          for {
            zAdd <- client.zAdd(key = key, ScoreWithValue(1, "one"))
            _ <- zAdd shouldBe 1L
            zAddWithNx <- client.zAdd(key = key, args = ZAddOptions(ZAddOptions.NX), ScoreWithValue(1, "one"))
            _ <- zAddWithNx shouldBe 0L
            zAddNewValueWithNx <- client.zAdd(key = key, args = ZAddOptions(ZAddOptions.NX), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five"))
            _ <- zAddNewValueWithNx shouldBe 4L
            zRange <- client.zRange(key, 0, -1)
            _ <- zRange shouldBe List("one", "two", "three", "four", "five")
            rangeWithScores <- client.zRangeWithScores(key, 0, 1)
            _ <- rangeWithScores.shouldBe(List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two")))
            rangeByScore <- client.zRangeByScore(key, ZRange(0, 2), Some(RangeLimit(0, 2)))
            _ <- rangeByScore.shouldBe(List("one", "two"))
            rangeByScoreWithScores <- client.zRangeByScoreWithScores(key, ZRange(0, 2), Some(RangeLimit(0, 2)))
            _ <- rangeByScoreWithScores.shouldBe(List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two")))
            revRange <- client.zRevRange(key, 0, -1)
            _ <- revRange shouldBe List("five", "four", "three", "two", "one")
            revRangeByScore <- client.zRevRangeByScore(key, ZRange(0, 2), Some(RangeLimit(0, 2)))
            _ <- revRangeByScore.shouldBe(List("two", "one"))
            revRangeWithScores <- client.zRevRangeWithScores(key, 0, -1)
            _ <- revRangeWithScores shouldBe List(ScoreWithValue(5, "five"), ScoreWithValue(4, "four"), ScoreWithValue(3, "three"), ScoreWithValue(2, "two"), ScoreWithValue(1, "one"))
            zCard <- client.zCard(key)
            _ <- zCard shouldBe 5L
            zCount <- client.zCount(key, ZRange(0, 2))
            _ <- zCount shouldBe 2L
            zScan <- client.zScan(key)
            _ <- zScan.finished shouldBe true
            _ <- zScan.values shouldBe List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five"))
            zScanWithMatch <- client.zScan(key, matchPattern = Some("t*"))
            _ <- zScanWithMatch.finished shouldBe true
            _ <- zScanWithMatch.values shouldBe List(ScoreWithValue(2, "two"), ScoreWithValue(3, "three"))
            zScanWithLimit <- client.zScan(key, limit = Some(10))
            _ <- zScanWithLimit.finished shouldBe true
            _ <- zScanWithLimit.values shouldBe List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five"))
            zScanWithMatchAndLimit <- client.zScan(key, matchPattern = Some("t*"), limit = Some(10))
            _ <- zScanWithMatchAndLimit.finished shouldBe true
            _ <- zScanWithMatchAndLimit.values shouldBe List(ScoreWithValue(2, "two"), ScoreWithValue(3, "three"))
            zRemRangeByRank <- client.zRemRangeByRank(key, 0, 0)
            _ <- zRemRangeByRank shouldBe 1L
            zRemRangeByScore <- client.zRemRangeByScore(key, ZRange(2, 2))
            _ <- zRemRangeByScore shouldBe 1L
            zPopMin <- client.zPopMin(key, 1)
            _ <- zPopMin.headOption shouldBe Some(ScoreWithValue(3, "three"))
            zPopMax <- client.zPopMax(key, 1)
            _ <- zPopMax.headOption shouldBe Some(ScoreWithValue(5, "five"))
            zRem <- client.zRem(key, "four")
            _ <- zRem shouldBe 1L
            endState <- client.zRangeWithScores(key, 0, -1)
            _ <- endState.isEmpty shouldBe true
          } yield succeed
        }
      }

      "support random key operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("sorted-set-random")
          for {
            _ <- client.zAdd(key, ScoreWithValue(1, "one"), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five"))
            randomKey <- client.zRandMember(key)
            _ <- randomKey.isDefined shouldBe true
            randomKeys <- client.zRandMember(key, 3)
            _ <- randomKeys.size shouldBe 3
            randomKeyWithValue <- client.zRandMemberWithScores(key)
            _ <- randomKeyWithValue.isDefined shouldBe true
            randomKeysWithValues <- client.zRandMemberWithScores(key, 3)
            _ <- randomKeysWithValues.size shouldBe 3
          } yield succeed
        }
      }

      "support increment operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("sorted-set-incr")
          for {
            _ <- client.zAdd(key, ScoreWithValue(1, "one"), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"))
            zCard <- client.zCard(key)
            _ <- zCard shouldBe 3L
            zCount <- client.zCount(key, ZRange(0, 2))
            _ <- zCount shouldBe 2L
            incrResult <- client.zIncrBy(key, 2, "two")
            _ <- incrResult shouldBe 4.0
            incrResult <- client.zIncrBy(key, 2, "four")
            _ <- incrResult shouldBe 2.0
            getResult <- client.zScore(key, "four")
            _ <- getResult shouldBe Some(2.0)
            zRankResult <- client.zRank(key, "four")
            _ <- zRankResult shouldBe Some(1L)
            zRankWithScore <- client.zRankWithScore(key, "four")
            _ <- zRankWithScore shouldBe Some(ScoreWithValue(2.0, 1))
            zRevRankResult <- client.zRevRank(key, "four")
            _ <- zRevRankResult shouldBe Some(2L)
            zRevRankWithScore <- client.zRevRankWithScore(key, "four")
            _ <- zRevRankWithScore shouldBe Some(ScoreWithValue(2.0, 2))
            zAddIncr <- client.zAddIncr(key, ZAddOptions(ZAddOptions.XX), 2.9, "four")
            zmScore <- client.zMScore(key, "four", "two")
            _ <- zmScore shouldBe List(Some(4.9), Some(4.0))
            _ <- zAddIncr shouldBe Some(4.9)
          } yield succeed
        }
      }

      "support multi-key operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val suffix = "{user1}"
          val key1 = randomKey("sorted-set1") + suffix
          val key2 = randomKey("sorted-set2") + suffix
          val key3 = randomKey("sorted-set3") + suffix
          val destination = randomKey("sorted-set-destination") + suffix
          for {
            _ <- client.zAdd(key1, ScoreWithValue(1, "one"), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"))
            _ <- client.zAdd(key2, ScoreWithValue(4, "four"), ScoreWithValue(5, "five"), ScoreWithValue(6, "six"))
            _ <- client.zAdd(key3, ScoreWithValue(7, "seven"), ScoreWithValue(8, "eight"), ScoreWithValue(9, "nine"))
            zrangeStore <- client.zRangeStore(destination, key1, 0, -1)
            _ <- zrangeStore shouldBe 3L
            bzPopMin <- client.bzPopMin(100.milliseconds, key1, key2, key3)
            _ <- bzPopMin shouldBe Some(ScoreWithKeyValue(1, key1, "one"))
            bzPopMax <- client.bzPopMax(100.milliseconds, key3, key2, key1)
            _ <- bzPopMax shouldBe Some(ScoreWithKeyValue(9, key3, "nine"))
            bzMPop <- client.bzMPop(100.milliseconds, SortOrder.Min, key1, key2, key3)
            _ <- bzMPop shouldBe Some(ScoreWithKeyValue(2, key1, "two"))
            bzMpopWithCount <- client.bzMPop(100.milliseconds, 2, SortOrder.Max, key3, key2, key1)
            _ <- bzMpopWithCount shouldBe List(ScoreWithKeyValue(8.0, key3, "eight"), ScoreWithKeyValue(7.0, key3, "seven"))
            zMPop <- client.zMPop(SortOrder.Min, key2, key3)
            _ <- zMPop shouldBe Some(ScoreWithKeyValue(4.0, key2, "four"))
            zMPopWithCount <- client.zMPop(2, SortOrder.Max, key3, key2)
            _ <- zMPopWithCount shouldBe List(ScoreWithKeyValue(6.0, key2, "six"), ScoreWithKeyValue(5.0, key2, "five"))


          } yield succeed
        }
      }

      "support lexographical range operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val suffix = "{user1}"
          val key = randomKey("sorted-set-lex") + suffix
          val destination = randomKey("sorted-set-destination") + suffix
          for {
            _ <- client.zAdd(key, ScoreWithValue(1, "a"), ScoreWithValue(2, "b"), ScoreWithValue(3, "c"), ScoreWithValue(4, "d"), ScoreWithValue(5, "e"))
            lexRange <- client.zRangeByLex(key, ZRange("a", "c"))
            _ <- lexRange shouldBe List("a", "b", "c")
            lexRange <- client.zRangeByLex(key, ZRange("a", "c"), Some(RangeLimit(0, 2)))
            _ <- lexRange shouldBe List("a", "b")
            revLexRange <- client.zRevRangeByLex(key, ZRange("a", "c"))
            _ <- revLexRange shouldBe List("c", "b", "a")
            _ <- client.zRangeStoreByLex(destination, key, ZRange("a", "c"))
            destinationRange <- client.zRange(destination, 0, -1)
            _ <- destinationRange shouldBe List("a", "b", "c")
          } yield succeed
        }
      }

      "support diff, inter and union operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val suffix = "{user1}"
          val key1 = randomKey("sorted-set1") + suffix
          val key2 = randomKey("sorted-set2") + suffix
          val key3 = randomKey("sorted-set3") + suffix
          val destination = randomKey("sorted-set-destination") + suffix
          for {
            _ <- client.zAdd(key1, ScoreWithValue(1, "a"), ScoreWithValue(2, "b"), ScoreWithValue(3, "c"), ScoreWithValue(4, "d"), ScoreWithValue(5, "e"))
            _ <- client.zAdd(key2, ScoreWithValue(1, "a"), ScoreWithValue(2, "b"), ScoreWithValue(3, "c"), ScoreWithValue(4, "d"), ScoreWithValue(5, "e"))
            _ <- client.zAdd(key3, ScoreWithValue(1, "a"), ScoreWithValue(2, "b"), ScoreWithValue(3, "c"), ScoreWithValue(4, "d"), ScoreWithValue(5, "e"))
            zInterStore <- client.zInterStore(destination, key1, key2, key3)
            _ <- zInterStore shouldBe 5L
            zInterCard <- client.zInterCard(key1, key2, key3)
            _ <- zInterCard shouldBe 5L
            zInterRange <- client.zRange(destination, 0, -1)
            _ <- zInterRange shouldBe List("a", "b", "c", "d", "e")
            zUnion <- client.zUnion(key1, key2)
            _ <- zUnion shouldBe List("a", "b", "c", "d", "e")
            zUnionWithScores <- client.zUnionWithScores(key1, key2)
            _ <- zUnionWithScores shouldBe List(ScoreWithValue(2.0, "a"), ScoreWithValue(4.0, "b"), ScoreWithValue(6.0, "c"), ScoreWithValue(8.0, "d"), ScoreWithValue(10.0, "e"))
            zUnionWithScoresAndWeights <- client.zUnionWithScores(AggregationArgs(weights = Seq(2.0, 3.0)), key1, key2)
            _ <- zUnionWithScoresAndWeights shouldBe List(ScoreWithValue(5.0, "a"), ScoreWithValue(10.0, "b"), ScoreWithValue(15.0, "c"), ScoreWithValue(20.0, "d"), ScoreWithValue(25.0, "e"))
            zUnionWithScoresMin <- client.zUnionWithScores(AggregationArgs(Aggregate.Min), key1, key2)
            _ <- zUnionWithScoresMin shouldBe List(ScoreWithValue(1.0, "a"), ScoreWithValue(2.0, "b"), ScoreWithValue(3.0, "c"), ScoreWithValue(4.0, "d"), ScoreWithValue(5.0, "e"))
            zUnionStore <- client.zUnionStore(destination, key1, key2, key3)
            _ <- zUnionStore shouldBe 5L
            zUnionRange <- client.zRange(destination, 0, -1)
            _ <- zUnionRange shouldBe List("a", "b", "c", "d", "e")
            zDiffStore <- client.zDiffStore(destination, key1, key2, key3)
            _ <- zDiffStore shouldBe 0L
            zDiffRange <- client.zRange(destination, 0, -1)
            _ <- zDiffRange shouldBe List()
          } yield succeed
        }
      }
    }

    "leveraging RedisHashAsyncCommands" should {

      "create, retrieve, and delete a field with a string value for a hash key" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("hash-key")
          val field = "field"
          val value = "value"

          for {
            _ <- client.hSet(key, field, value)
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe true
            existingKeyAdded <- client.hSetNx(key, field, value)
            _ <- existingKeyAdded shouldBe false
            newKeyAdded <- client.hSetNx("newKey", field, value)
            _ <- newKeyAdded shouldBe true
            fieldValue <- client.hGet(key, field)
            _ <- fieldValue match {
              case Some(expectedFieldValue) => expectedFieldValue shouldBe value
              case None => fail()
            }
            deleted <- client.hDel(key, field)
            _ <- deleted shouldBe 1L
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe false
            _ <- client.hMSet(key, Map("field1" -> "value1", "field2" -> "value2", "field3" -> "value3"))
            fieldValues <- client.hGetAll(key)
            _ <- fieldValues shouldBe Map("field1" -> "value1", "field2" -> "value2", "field3" -> "value3")

          } yield succeed
        }
      }

      "support complex hash operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("hash-key")
          val field = "field"
          val value = "value"
          for {
            _ <- client.hSet(key, field, value)
            fieldExists <- client.hExists(key, field)
            _ <- fieldExists shouldBe true
            randomField <- client.hRandField(key)
            _ <- randomField shouldBe Some(field)
            randomFields <- client.hRandField(key, 2)
            _ <- randomFields shouldBe Seq(field)
            randomFieldWithValue <- client.hRandFieldWithValues(key)
            _ <- randomFieldWithValue shouldBe Some(field -> value)
            fieldValues <- client.hKeys(key)
            _ <- fieldValues shouldBe Seq(field)
            fieldValues <- client.hGetAll(key)
            _ <- fieldValues shouldBe Map(field -> value)
            fieldValues <- client.hVals(key)
            _ <- fieldValues shouldBe Seq(value)
            len <- client.hLen(key)
            _ <- len shouldBe 1L
            hStrLen <- client.hStrLen(key, field)
            _ <- hStrLen shouldBe 5L
            hScanResults <- client.hScan(key)
            _ <- hScanResults.finished shouldBe true
            _ <- hScanResults.values shouldBe Map(field -> value)
            _ <- client.del(key)
            _ <- client.hMSet(key, Map("field1" -> "value1", "field2" -> "value2", "extraField3" -> "value3"))
            fieldValues <- client.hGetAll(key)
            _ <- fieldValues shouldBe Map("field1" -> "value1", "field2" -> "value2", "extraField3" -> "value3")
            scanResultsWithFilter <- client.hScan(key, matchPattern = Some("field*"))
            _ <- scanResultsWithFilter.finished shouldBe true
            _ <- scanResultsWithFilter.values shouldBe Map("field1" -> "value1", "field2" -> "value2")
            scanResultsWithLimit <- client.hScan(key, limit = Some(10))
            _ <- scanResultsWithLimit.finished shouldBe true
            _ <- scanResultsWithLimit.values shouldBe Map("field1" -> "value1", "field2" -> "value2", "extraField3" -> "value3")
            randomFieldsWithValues <- client.hRandFieldWithValues(key, 3)
            _ <- randomFieldsWithValues shouldBe Map("field1" -> "value1", "field2" -> "value2", "extraField3" -> "value3")
          } yield succeed
        }
      }

      "create, retrieve, and delete a field with an integer value for a hash key" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("int-hash-key")
          val field = "field"
          val value = 1

          for {
            _ <- client.hIncrBy(key, field, 1L)
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe true
            fieldValue <- client.hGet(key, field)
            _ <- fieldValue match {
              case Some(expectedFieldValue) => expectedFieldValue.toInt shouldBe value
              case None => fail()
            }
            deleted <- client.hDel(key, field)
            _ <- deleted shouldBe 1L
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe false
          } yield succeed
        }
      }
    }

    "leveraging RedisSetAsyncCommands" should {
      "create, retrieve, pop, and remove values in a set" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("set")
          val values = List("one", "two", "three")
          for {
            addResults <- client.sAdd(key, values: _*)
            _ <- addResults shouldBe values.size
            members <- client.sMembers(key)
            _ <- members shouldBe values.toSet
            isMember <- client.sIsMember(key, "two")
            _ <- isMember shouldBe true
            isMember <- client.sIsMember(key, "four")
            _ <- isMember shouldBe false
            multiIsMember <- client.smIsMember(key, "one", "two", "three", "four")
            _ <- multiIsMember shouldBe List(true, true, true, false)
            cardResult <- client.sCard(key)
            _ <- cardResult shouldBe values.size
            popResult <- client.sPop(key)
            _ <- popResult shouldBe defined
            removeResult <- client.sRem(key, values: _*)
            _ <- removeResult shouldBe 2
          } yield succeed
        }
      }

      "support random member operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("set")
          val values = List("one", "two", "three")
          for {
            addResults <- client.sAdd(key, values: _*)
            _ <- addResults shouldBe values.size
            randResult <- client.sRandMember(key)
            _ <- randResult shouldBe defined
            randResults <- client.sRandMember(key, 2)
            _ <- randResults.size shouldBe 2
          } yield succeed
        }
      }

      "support multi-key union, diffing, and moving operations" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val suffix = "{user1}"
          val key1 = randomKey("set-1") + suffix
          val key2 = randomKey("set-2") + suffix
          val key3 = randomKey("set-3") + suffix
          val values = List("one", "two", "three")
          for {
            addResults <- client.sAdd(key1, values: _*)
            _ <- addResults shouldBe values.size
            addResults <- client.sAdd(key2, values: _*)
            _ <- addResults shouldBe values.size
            addResults <- client.sAdd(key3, values: _*)
            _ <- addResults shouldBe values.size
            sDiffResults <- client.sDiff(key1, key2, key3)
            _ <- sDiffResults shouldBe Set()
            sInterResults <- client.sInter(key1, key2, key3)
            _ <- sInterResults shouldBe values.toSet
            sUnionResults <- client.sUnion(key1, key2, key3)
            _ <- sUnionResults shouldBe values.toSet
            sDiffStoreResults <- client.sDiffStore(key1, key2, key3, key3)
            _ <- sDiffStoreResults shouldBe 0
            sInterStoreResults <- client.sInterStore(key1, key2, key3, key3)
            _ <- sInterStoreResults shouldBe values.size
            sUnionStoreResults <- client.sUnionStore(key1, key2, key3, key3)
            _ <- sUnionStoreResults shouldBe values.size
            interCardResult <- client.sInterCard(key1, key2, key3)
            _ <- interCardResult shouldBe values.size
            moveResult <- client.sMove(key1, key2, "two")
            _ <- moveResult shouldBe true
            diffResult <- client.sDiff(key2, key1)
            _ <- diffResult shouldBe Set("two")
            unionResult <- client.sUnion(key1, key2)
            _ <- unionResult shouldBe values.toSet
          } yield succeed
        }
      }


      "support scanning a set" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("large-set")
          val members = (1 to 1000).map(_.toString).toList

          def scanAll(cursor: String = InitialCursor, accumulated: Set[String] = Set.empty): Future[Set[String]] = {
            client.sScan(key, cursor).flatMap { scanResult =>
              val newAccumulated = accumulated ++ scanResult.values
              if (scanResult.finished) Future.successful(newAccumulated)
              else scanAll(scanResult.cursor, newAccumulated)
            }
          }

          for {
            addResults <- client.sAdd(key, members: _*)
            _ <- addResults shouldBe members.size
            scanResult <- client.sScan(key)
            _ <- scanResult.finished shouldBe false
            _ <- scanResult.values.size shouldBe >=(1)
            scanResultNext <- client.sScan(key, cursor = scanResult.cursor)
            _ <- scanResultNext.finished shouldBe false
            _ <- scanResultNext.values.size shouldBe >=(2)
            scanResultWithMatch <- client.sScan(key, matchPattern = Some("1*"))
            _ <- scanResultWithMatch.finished shouldBe false
            _ <- members.filter(_.startsWith("1")).toSet should contain allElementsOf scanResultWithMatch.values
            scanResultWithLimit <- client.sScan(key, limit = Some(3))
            _ <- scanResultWithLimit.finished shouldBe false
            _ <- members should contain allElementsOf scanResultWithLimit.values

            // start with initial cursor and fetch until finished
            scanResult <- scanAll()
            _ <- scanResult.size shouldEqual members.size
            _ <- scanResult shouldBe members.toSet
          } yield succeed
        }
      }
    }

    "leveraging RedisPipelineAsyncCommands" should {

      "allow to send a batch of commands using pipeline" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("pipeline-key")
          val field = "field"
          val value = "value"
          val expireIn = 30.minutes

          for {
            pipelineOutcome <- client.pipeline(redisCommands => List(
              redisCommands.hSet(key, field, value),
              redisCommands.hSet(key, field, value), //should not create key as it already exists
              redisCommands.expire(key, expireIn)
            ))
            _ <- pipelineOutcome match {
              case Some(operationsOutcome) => operationsOutcome shouldBe List(true, false, true)
              case None => fail()
            }
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe true
            ttl <- client.ttl(key)
            _ <- ttl match {
              case Some(timeToLive) => assert(timeToLive > (expireIn - 1.minute) && timeToLive <= expireIn)
              case None => fail()
            }
          } yield succeed
        }
      }
    }

    "leveraging RedisServerAsyncCommands" should {

      "allow to get information from server and retrieve at least 199 keys (single-node)" in {
        withRedisSingleNode(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          for {
            info <- client.info
            _ <- info.isEmpty shouldBe false
            _ <- assert(info.size >= 199)
          } yield succeed
        }
      }

      "allow to get information from server and retrieve at least 200 keys (cluster)" in {
        withRedisCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          for {
            info <- client.info
            _ <- info.isEmpty shouldBe false
            _ <- assert(info.size >= 200)
          } yield succeed
        }
      }

      "allow to flush all keys from all databases" in {
        //single node only for now as it produces a random error "READONLY You can't write against a read only replica" (port 7005) with the Redis cluster
        withRedisSingleNode(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey()
          val value = "value"
          val field = "field"
          for {
            allKeysAfterInit <- client.hGetAll(key)
            _ <- allKeysAfterInit.isEmpty shouldBe true
            _ <- client.hSet(key, field, value)
            allKeysAfterUpdate <- client.hGetAll(key)
            _ <- allKeysAfterUpdate.size shouldBe 1
            _ <- client.flushAll
            allKeysAfterFlushAll <- client.hGetAll(key)
            _ <- allKeysAfterFlushAll.isEmpty shouldBe true
          } yield succeed
        }
      }

    }

  }
}

object RedisCommandsIntegrationSpec{
  def randomKey(prefix: String = "key"): String = s"$prefix-${java.util.UUID.randomUUID()}"
}

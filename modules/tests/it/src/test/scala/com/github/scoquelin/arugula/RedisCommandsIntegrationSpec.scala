package com.github.scoquelin.arugula

import scala.collection.immutable.ListMap

import com.github.scoquelin.arugula.codec.RedisCodec
import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands.{RangeLimit, ScoreWithValue, ZAddOptions, ZRange}
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

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

          } yield succeed
        }
      }

    }

    "leveraging RedisSortedSetAsyncCommands" should {

      "create, retrieve, scan and delete values in a sorted set" in {
        withRedisSingleNodeAndCluster(RedisCodec.Utf8WithValueAsStringCodec) { client =>
          val key = randomKey("sorted-set")

          for {
            zAdd <- client.zAdd(key = key, args = None, ScoreWithValue(1, "one"))
            _ <- zAdd shouldBe 1L
            zAddWithNx <- client.zAdd(key = key, args = Some(ZAddOptions.NX), ScoreWithValue(1, "one"))
            _ <- zAddWithNx shouldBe 0L
            zAddNewValueWithNx <- client.zAdd(key = key, args = Some(ZAddOptions.NX), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five"))
            _ <- zAddNewValueWithNx shouldBe 4L
            rangeWithScores <- client.zRangeWithScores(key, 0, 1)
            _ <- rangeWithScores.shouldBe(List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two")))
            rangeByScore <- client.zRangeByScore(key, ZRange(0, 2), Some(RangeLimit(0, 2)))
            _ <- rangeByScore.shouldBe(List("one", "two"))
            revRangeByScore <- client.zRevRangeByScore(key, ZRange(0, 2), Some(RangeLimit(0, 2)))
            _ <- revRangeByScore.shouldBe(List("two", "one"))
            zScan <- client.zScan(key)
            _ <- zScan.cursor.finished shouldBe true
            _ <- zScan.values shouldBe List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five"))
            zScanWithMatch <- client.zScan(key, matchPattern = Some("t*"))
            _ <- zScanWithMatch.cursor.finished shouldBe true
            _ <- zScanWithMatch.values shouldBe List(ScoreWithValue(2, "two"), ScoreWithValue(3, "three"))
            zScanWithLimit <- client.zScan(key, limit = Some(10))
            _ <- zScanWithLimit.cursor.finished shouldBe true
            _ <- zScanWithLimit.values shouldBe List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five"))
            zScanWithMatchAndLimit <- client.zScan(key, matchPattern = Some("t*"), limit = Some(10))
            _ <- zScanWithMatchAndLimit.cursor.finished shouldBe true
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
            _ <- hScanResults._1.finished shouldBe true
            _ <- hScanResults._2 shouldBe Map(field -> value)
            _ <- client.del(key)
            _ <- client.hMSet(key, Map("field1" -> "value1", "field2" -> "value2", "extraField3" -> "value3"))
            fieldValues <- client.hGetAll(key)
            _ <- fieldValues shouldBe Map("field1" -> "value1", "field2" -> "value2", "extraField3" -> "value3")
            scanResultsWithFilter <- client.hScan(key, matchPattern = Some("field*"))
            _ <- scanResultsWithFilter._1.finished shouldBe true
            _ <- scanResultsWithFilter._2 shouldBe Map("field1" -> "value1", "field2" -> "value2")
            scanResultsWithLimit <- client.hScan(key, limit = Some(10))
            _ <- scanResultsWithLimit._1.finished shouldBe true
            _ <- scanResultsWithLimit._2 shouldBe Map("field1" -> "value1", "field2" -> "value2", "extraField3" -> "value3")
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

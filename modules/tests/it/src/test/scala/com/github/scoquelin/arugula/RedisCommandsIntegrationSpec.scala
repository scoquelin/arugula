package com.github.scoquelin.arugula

import com.github.scoquelin.arugula.commands.RedisSortedSetAsyncCommands.{RangeLimit, ScoreWithValue, ZAddOptions, ZRange}
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class RedisCommandsIntegrationSpec extends BaseRedisCommandsIntegrationSpec with Matchers {
  import RedisCommandsIntegrationSpec.randomKey

  "RedisCommandsClient" when {

    "leveraging RedisBaseAsyncCommands" should {

      "ping" in {
        withRedisSingleNodeAndCluster { client =>
          for {
            response <- client.ping
            _ <- response shouldBe "PONG"
          } yield succeed
        }
      }
    }

    "leveraging RedisStringAsyncCommands" should {

      "create, check, retrieve, and delete a key" in {
        withRedisSingleNodeAndCluster { client =>
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
              case None => fail
            }
            deleted <- client.del(key)
            _ <- deleted shouldBe 1L
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe false
          } yield succeed
        }
      }

      "create, check, retrieve, and delete a key with expiration" in {
        withRedisSingleNodeAndCluster { client =>
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
              case None => fail
            }
            ttl <- client.ttl(key)
            _ <- ttl match {
              case Some(timeToLive) => assert(timeToLive > (expireIn - 1.minute) && timeToLive <= expireIn)
              case None => fail
            }
            deleted <- client.del(key)
            _ <- deleted shouldBe 1L
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe false
          } yield succeed
        }
      }

    }

    "leveraging RedisListAsyncCommands" should {

      "create, retrieve, and delete values in a list" in {
        withRedisSingleNodeAndCluster { client =>
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
        withRedisSingleNodeAndCluster { client =>
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
        withRedisSingleNodeAndCluster { client =>
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
              case None => fail
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

      "create, retrieve, and delete a field with an integer value for a hash key" in {
        withRedisSingleNodeAndCluster { client =>
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
              case None => fail
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
        withRedisSingleNodeAndCluster { client =>
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
              case None => fail
            }
            keyExists <- client.exists(key)
            _ <- keyExists shouldBe true
            ttl <- client.ttl(key)
            _ <- ttl match {
              case Some(timeToLive) => assert(timeToLive > (expireIn - 1.minute) && timeToLive <= expireIn)
              case None => fail
            }
          } yield succeed
        }
      }
    }

    "leveraging RedisServerAsyncCommands" should {

      "allow to get information from server and retrieve at least 199 keys (single-node)" in {
        withRedisSingleNode { client =>
          for {
            info <- client.info
            _ <- info.isEmpty shouldBe false
            _ <- assert(info.size >= 199)
          } yield succeed
        }
      }

      "allow to get information from server and retrieve at least 200 keys (cluster)" in {
        withRedisCluster { client =>
          for {
            info <- client.info
            _ <- info.isEmpty shouldBe false
            _ <- assert(info.size >= 200)
          } yield succeed
        }
      }

      "allow to flush all keys from all databases" in {
        //single node only for now as it produces a random error "READONLY You can't write against a read only replica" (port 7005) with the Redis cluster
        withRedisSingleNode { client =>
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

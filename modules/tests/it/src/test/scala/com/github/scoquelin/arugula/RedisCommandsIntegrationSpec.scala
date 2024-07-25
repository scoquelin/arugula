package com.github.scoquelin.arugula

import com.github.scoquelin.arugula.api.commands.RedisSortedSetAsyncCommands.{RangeLimit, ScoreWithValue, ZAddOptions, ZRange}
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class RedisCommandsIntegrationSpec extends BaseRedisCommandsIntegrationSpec with Matchers {

  "RedisCommandsClient" when {

    "leveraging RedisBaseAsyncCommands" should {

      "ping" in {
        withRedisSingleNodeAndCluster { client =>
          for {
            response <- client.sendCommand(_.ping)
            _ <- response shouldBe "PONG"
          } yield succeed
        }
      }
    }

    "leveraging RedisStringAsyncCommands" should {

      "create, check, retrieve, and delete a key" in {
        withRedisSingleNodeAndCluster { client =>
          val key = "key"
          val value = "value"

          for {
            _ <- client.sendCommand(_.set(key, value))
            keyExists <- client.sendCommand(_.exists(key))
            _ <- keyExists shouldBe true
            existingKeyAdded <- client.sendCommand(_.setNx(key, value)) //noop since key already exists
            _ <- existingKeyAdded shouldBe false
            newKeyAdded <- client.sendCommand(_.setNx("newKey", value))
            _ <- newKeyAdded shouldBe true
            keyValue <- client.sendCommand(_.get(key))
            _ <- keyValue match {
              case Some(expectedValue) => expectedValue shouldBe value
              case None => fail
            }
            deleted <- client.sendCommand(_.del(key))
            _ <- deleted shouldBe 1L
            keyExists <- client.sendCommand(_.exists(key))
            _ <- keyExists shouldBe false
          } yield succeed
        }
      }

      "create, check, retrieve, and delete a key with expiration" in {
        withRedisSingleNodeAndCluster { client =>
          val key = "key"
          val value = "value"
          val expireIn = 30.minutes

          for {
            _ <- client.sendCommand(_.setEx(key, value, expireIn))
            keyExists <- client.sendCommand(_.exists(key))
            _ <- keyExists shouldBe true
            keyValue <- client.sendCommand(_.get(key))
            _ <- keyValue match {
              case Some(expectedValue) => expectedValue shouldBe value
              case None => fail
            }
            ttl <- client.sendCommand(_.ttl(key))
            _ <- ttl match {
              case Some(timeToLive) => assert(timeToLive > (expireIn - 1.minute) && timeToLive <= expireIn)
              case None => fail
            }
            deleted <- client.sendCommand(_.del(key))
            _ <- deleted shouldBe 1L
            keyExists <- client.sendCommand(_.exists(key))
            _ <- keyExists shouldBe false
          } yield succeed
        }
      }

    }

    "leveraging RedisSortedSetAsyncCommands" should {

      "create, retrieve, scan and delete values in a sorted set" in {
        withRedisSingleNodeAndCluster { client =>
          val key = "key"

          for {
            zAdd <- client.sendCommand(_.zAdd(key = key, args = None, ScoreWithValue(1, "one")))
            _ <- zAdd shouldBe 1L
            zAddWithNx <- client.sendCommand(_.zAdd(key = key, args = Some(ZAddOptions.NX), ScoreWithValue(1, "one")))
            _ <- zAddWithNx shouldBe 0L
            zAddNewValueWithNx <- client.sendCommand(_.zAdd(key = key, args = Some(ZAddOptions.NX), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five")))
            _ <- zAddNewValueWithNx shouldBe 4L
            rangeWithScores <- client.sendCommand(_.zRangeWithScores(key, 0, 1))
            _ <- rangeWithScores.shouldBe(List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two")))
            rangeByScore <- client.sendCommand(_.zRangeByScore(key, ZRange(0, 2), Some(RangeLimit(0, 2))))
            _ <- rangeByScore.shouldBe(List("one", "two"))
            revRangeByScore <- client.sendCommand(_.zRevRangeByScore(key, ZRange(0, 2), Some(RangeLimit(0, 2))))
            _ <- revRangeByScore.shouldBe(List("two", "one"))
            zScan <- client.sendCommand(_.zScan(key))
            _ <- zScan.cursor.finished shouldBe true
            _ <- zScan.values shouldBe List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five"))
            zScanWithMatch <- client.sendCommand(_.zScan(key, matchPattern = Some("t*")))
            _ <- zScanWithMatch.cursor.finished shouldBe true
            _ <- zScanWithMatch.values shouldBe List(ScoreWithValue(2, "two"), ScoreWithValue(3, "three"))
            zScanWithLimit <- client.sendCommand(_.zScan(key, limit = Some(10)))
            _ <- zScanWithLimit.cursor.finished shouldBe true
            _ <- zScanWithLimit.values shouldBe List(ScoreWithValue(1, "one"), ScoreWithValue(2, "two"), ScoreWithValue(3, "three"), ScoreWithValue(4, "four"), ScoreWithValue(5, "five"))
            zScanWithMatchAndLimit <- client.sendCommand(_.zScan(key, matchPattern = Some("t*"), limit = Some(10)))
            _ <- zScanWithMatchAndLimit.cursor.finished shouldBe true
            _ <- zScanWithMatchAndLimit.values shouldBe List(ScoreWithValue(2, "two"), ScoreWithValue(3, "three"))
            zRemRangeByRank <- client.sendCommand(_.zRemRangeByRank(key, 0, 0))
            _ <- zRemRangeByRank shouldBe 1L
            zRemRangeByScore <- client.sendCommand(_.zRemRangeByScore(key, ZRange(2, 2)))
            _ <- zRemRangeByScore shouldBe 1L
            zPopMin <- client.sendCommand(_.zPopMin(key, 1))
            _ <- zPopMin.headOption shouldBe Some(ScoreWithValue(3, "three"))
            zPopMax <- client.sendCommand(_.zPopMax(key, 1))
            _ <- zPopMax.headOption shouldBe Some(ScoreWithValue(5, "five"))
            zRem <- client.sendCommand(_.zRem(key, "four"))
            _ <- zRem shouldBe 1L

            endState <- client.sendCommand(_.zRangeWithScores(key, 0, -1))
            _ <- endState.isEmpty shouldBe true
          } yield succeed
        }
      }
    }

    "leveraging RedisHashAsyncCommands" should {

      "create, retrieve, and delete a field with a string value for a hash key" in {
        withRedisSingleNodeAndCluster { client =>
          val key = "key"
          val field = "field"
          val value = "value"

          for {
            _ <- client.sendCommand(_.hSet(key, field, value))
            keyExists <- client.sendCommand(_.exists(key))
            _ <- keyExists shouldBe true
            existingKeyAdded <- client.sendCommand(_.hSetNx(key, field, value))
            _ <- existingKeyAdded shouldBe false
            newKeyAdded <- client.sendCommand(_.hSetNx("newKey", field, value))
            _ <- newKeyAdded shouldBe true
            fieldValue <- client.sendCommand(_.hGet(key, field))
            _ <- fieldValue match {
              case Some(expectedFieldValue) => expectedFieldValue shouldBe value
              case None => fail
            }
            deleted <- client.sendCommand(_.hDel(key, field))
            _ <- deleted shouldBe 1L
            keyExists <- client.sendCommand(_.exists(key))
            _ <- keyExists shouldBe false
          } yield succeed
        }
      }

      "create, retrieve, and delete a field with an integer value for a hash key" in {
        withRedisSingleNodeAndCluster { client =>
          val key = "key"
          val field = "field"
          val value = 1

          for {
            _ <- client.sendCommand(_.hIncrBy(key, field, 1L))
            keyExists <- client.sendCommand(_.exists(key))
            _ <- keyExists shouldBe true
            fieldValue <- client.sendCommand(_.hGet(key, field))
            _ <- fieldValue match {
              case Some(expectedFieldValue) => expectedFieldValue.toInt shouldBe value
              case None => fail
            }
            deleted <- client.sendCommand(_.hDel(key, field))
            _ <- deleted shouldBe 1L
            keyExists <- client.sendCommand(_.exists(key))
            _ <- keyExists shouldBe false
          } yield succeed
        }
      }

    }

    "leveraging RedisPipelineAsyncCommands" should {

      "allow to send a batch of commands using pipeline" in {
        withRedisSingleNodeAndCluster { client =>
          val key = "key"
          val field = "field"
          val value = "value"
          val expireIn = 30.minutes

          for {
            pipelineOutcome <- client.sendCommand(_.pipeline(redisCommands => List(
              redisCommands.hSet(key, field, value),
              redisCommands.hSet(key, field, value), //should not create key as it already exists
              redisCommands.expire(key, expireIn)
            )))
            _ <- pipelineOutcome match {
              case Some(operationsOutcome) => operationsOutcome shouldBe List(true, false, true)
              case None => fail
            }
            keyExists <- client.sendCommand(_.exists(key))
            _ <- keyExists shouldBe true
            ttl <- client.sendCommand(_.ttl(key))
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
            info <- client.sendCommand(_.info)
            _ <- info.isEmpty shouldBe false
            _ <- assert(info.size >= 199)
          } yield succeed
        }
      }

      "allow to get information from server and retrieve at least 200 keys (cluster)" in {
        withRedisCluster { client =>
          for {
            info <- client.sendCommand(_.info)
            _ <- info.isEmpty shouldBe false
            _ <- assert(info.size >= 200)
          } yield succeed
        }
      }

      "allow to flush all keys from all databases" in {
        //single node only for now as it produces a random error "READONLY You can't write against a read only replica" (port 7005) with the Redis cluster
        withRedisSingleNode { client =>
          val key = "key"
          val value = "value"
          val field = "field"
          for {
            allKeysAfterInit <- client.sendCommand(_.hGetAll(key))
            _ <- allKeysAfterInit.isEmpty shouldBe true
            _ <- client.sendCommand(_.hSet(key, field, value))
            allKeysAfterUpdate <- client.sendCommand(_.hGetAll(key))
            _ <- allKeysAfterUpdate.size shouldBe 1
            _ <- client.sendCommand(_.flushAll)
            allKeysAfterFlushAll <- client.sendCommand(_.hGetAll(key))
            _ <- allKeysAfterFlushAll.isEmpty shouldBe true
          } yield succeed
        }
      }

    }

  }
}

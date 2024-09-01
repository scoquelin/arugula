package com.github.scoquelin.arugula

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.MapHasAsScala

import com.github.scoquelin.arugula.commands.RedisServerAsyncCommands
import io.lettuce.core.RedisFuture
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

class LettuceRedisServerAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisServerAsyncCommands" should {

    "delegate BGREWRITEAOF command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.bgrewriteaof()).thenReturn(mockRedisFuture)

      testClass.bgRewriteAof.map { _ =>
        verify(lettuceAsyncCommands).bgrewriteaof()
        succeed
      }
    }

    "delegate BGSAVE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.bgsave()).thenReturn(mockRedisFuture)

      testClass.bgSave.map { _ =>
        verify(lettuceAsyncCommands).bgsave()
        succeed
      }
    }

    "delegate CLIENT CACHING command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.clientCaching(true)).thenReturn(mockRedisFuture)

      testClass.clientCaching().map { _ =>
        verify(lettuceAsyncCommands).clientCaching(true)
        succeed
      }
    }

    "delegate CLIENT GETNAME command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "clientName"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.clientGetname()).thenReturn(mockRedisFuture)

      testClass.clientGetName.map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).clientGetname()
        succeed
      }
    }

    "delegate CLIENT GETREDIR command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.clientGetredir()).thenReturn(mockRedisFuture)

      testClass.clientGetRedir.map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).clientGetredir()
        succeed
      }
    }

    "delegate CLIENT ID command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.clientId()).thenReturn(mockRedisFuture)

      testClass.clientId.map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).clientId()
        succeed
      }
    }

    "delegate CLIENT KILL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "OK"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.clientKill("address")).thenReturn(mockRedisFuture)

      testClass.clientKill("address").map { _ =>
        verify(lettuceAsyncCommands).clientKill("address")
        succeed
      }
    }

    "delegate CLIENT KILL command with args to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.clientKill(any[io.lettuce.core.KillArgs])).thenReturn(mockRedisFuture)

      testClass.clientKill(RedisServerAsyncCommands.KillArgs()).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).clientKill(any[io.lettuce.core.KillArgs])
        succeed
      }
    }

    "delegate CLIENT LIST command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = List("id=74 addr=192.168.65.1:43101 laddr=172.18.0.4:6379 fd=8 name= age=3 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=40928 argv-mem=10 obl=0 oll=0 omem=0 tot-mem=61466 events=r cmd=client user=default redir=-1")
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue.mkString("\n"))
      when(lettuceAsyncCommands.clientList()).thenReturn(mockRedisFuture)

      testClass.clientList.map { result =>
        result mustBe expectedValue.map(RedisServerAsyncCommands.Client.parseClientInfo(_).get)
        verify(lettuceAsyncCommands).clientList()
        succeed
      }
    }

    "delegate CLIENT LIST command with args to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = List("id=74 addr=192.168.65.1:43101 laddr=172.18.0.4:6379 fd=8 name= age=3 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=40928 argv-mem=10 obl=0 oll=0 omem=0 tot-mem=61466 events=r cmd=client user=default redir=-1")
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue.mkString("\n"))
      when(lettuceAsyncCommands.clientList(any[io.lettuce.core.ClientListArgs])).thenReturn(mockRedisFuture)

      testClass.clientList(
        RedisServerAsyncCommands.ClientListArgs(
          ids = List(74L),
        )
      ).map { result =>
        result mustBe expectedValue.map(RedisServerAsyncCommands.Client.parseClientInfo(_).get)
        verify(lettuceAsyncCommands).clientList(any[io.lettuce.core.ClientListArgs])
        succeed
      }
    }

    "delegate CLIENT INFO command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "id=74 addr=192.168.65.1:43101 laddr=172.18.0.4:6379 fd=8 name= age=3 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=40928 argv-mem=10 obl=0 oll=0 omem=0 tot-mem=61466 events=r cmd=client user=default redir=-1"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.clientInfo()).thenReturn(mockRedisFuture)

      testClass.clientInfo.map { result =>
        result mustBe RedisServerAsyncCommands.Client.parseClientInfo(expectedValue).get
        verify(lettuceAsyncCommands).clientInfo()
        succeed
      }
    }

    "delegate CLIENT NO-EVICT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.clientNoEvict(true)).thenReturn(mockRedisFuture)

      testClass.clientNoEvict().map { _ =>
        verify(lettuceAsyncCommands).clientNoEvict(true)
        succeed
      }
    }

    "delegate CLIENT PAUSE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.clientPause(1000L)).thenReturn(mockRedisFuture)

      testClass.clientPause(1000.milliseconds).map { _ =>
        verify(lettuceAsyncCommands).clientPause(1000L)
        succeed
      }
    }

    "delegate CLIENT SETNAME command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.clientSetname("clientName")).thenReturn(mockRedisFuture)

      testClass.clientSetName("clientName").map { _ =>
        verify(lettuceAsyncCommands).clientSetname("clientName")
        succeed
      }
    }

    "delegate CLIENT SETINFO command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.clientSetinfo("param", "value")).thenReturn(mockRedisFuture)

      testClass.clientSetInfo("param", "value").map { _ =>
        verify(lettuceAsyncCommands).clientSetinfo("param", "value")
        succeed
      }
    }

    "delegate CLIENT TRACKING command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.clientTracking(any[io.lettuce.core.TrackingArgs])).thenReturn(mockRedisFuture)

      testClass.clientTracking(RedisServerAsyncCommands.TrackingArgs(
        enabled = true
      )).map { _ =>
        verify(lettuceAsyncCommands).clientTracking(any[io.lettuce.core.TrackingArgs])
        succeed
      }
    }

    "delegate CLIENT UNBLOCK command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.clientUnblock(1L, io.lettuce.core.UnblockType.ERROR)).thenReturn(mockRedisFuture)

      testClass.clientUnblock(1L, RedisServerAsyncCommands.UnblockType.Error).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).clientUnblock(1L, io.lettuce.core.UnblockType.ERROR)
        succeed
      }
    }

    // TODO: need to test command() and commandInfo() methods but integration tests are needed to do so.

    "delegate COMMAND COUNT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.commandCount()).thenReturn(mockRedisFuture)

      testClass.commandCount.map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).commandCount()
        succeed
      }
    }

    "delegate CONFIG GET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.Map.of("param", "value")
      val mockRedisFuture: RedisFuture[java.util.Map[String, String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.configGet(List("param"): _*)).thenReturn(mockRedisFuture)

      testClass.configGet("param").map { result =>
        result mustBe expectedValue.asScala.toMap
        verify(lettuceAsyncCommands).configGet(List("param"): _*)
        succeed
      }
    }

    "delegate CONFIG RESETSTAT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.configResetstat()).thenReturn(mockRedisFuture)

      testClass.configResetStat.map { _ =>
        verify(lettuceAsyncCommands).configResetstat()
        succeed
      }
    }

    "delegate CONFIG REWRITE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.configRewrite()).thenReturn(mockRedisFuture)

      testClass.configRewrite.map { _ =>
        verify(lettuceAsyncCommands).configRewrite()
        succeed
      }
    }

    "delegate CONFIG SET command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.configSet("param", "value")).thenReturn(mockRedisFuture)

      testClass.configSet("param", "value").map { _ =>
        verify(lettuceAsyncCommands).configSet("param", "value")
        succeed
      }
    }

    "delegate CONFIG SET command with map to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      val input = java.util.Map.of("param", "value")
      when(lettuceAsyncCommands.configSet(input)).thenReturn(mockRedisFuture)

      testClass.configSet(Map("param" -> "value")).map { _ =>
        verify(lettuceAsyncCommands).configSet(input)
        succeed
      }
    }

    "delegate DBSIZE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.dbsize()).thenReturn(mockRedisFuture)

      testClass.dbSize.map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).dbsize()
        succeed
      }
    }

    "delegate FLUSHALL command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.flushall(io.lettuce.core.FlushMode.SYNC)).thenReturn(mockRedisFuture)

      testClass.flushAll().map { _ =>
        verify(lettuceAsyncCommands).flushall(io.lettuce.core.FlushMode.SYNC)
        succeed
      }
    }

    "delegate FLUSHDB command with ASYNC to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.flushdb(io.lettuce.core.FlushMode.ASYNC)).thenReturn(mockRedisFuture)

      testClass.flushDb(RedisServerAsyncCommands.FlushMode.Async).map { _ =>
        verify(lettuceAsyncCommands).flushdb(io.lettuce.core.FlushMode.ASYNC)
        succeed
      }
    }

    "delegate FLUSHDB command with SYNC to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.flushdb(io.lettuce.core.FlushMode.SYNC)).thenReturn(mockRedisFuture)

      testClass.flushDb(RedisServerAsyncCommands.FlushMode.Sync).map { _ =>
        verify(lettuceAsyncCommands).flushdb(io.lettuce.core.FlushMode.SYNC)
        succeed
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

    "delegate LASTSAVE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val yesterday = java.time.Instant.now.minusSeconds(86400)
      val expectedValue = java.util.Date.from(yesterday)
      val mockRedisFuture: RedisFuture[java.util.Date] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.lastsave()).thenReturn(mockRedisFuture)

      testClass.lastSave.map { result =>
        result mustBe expectedValue.toInstant
        verify(lettuceAsyncCommands).lastsave()
        succeed
      }
    }

    "delegate MEMORY USAGE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.memoryUsage("key")).thenReturn(mockRedisFuture)

      testClass.memoryUsage("key").map { result =>
        result mustBe Some(expectedValue)
        verify(lettuceAsyncCommands).memoryUsage("key")
        succeed
      }
    }

    "delegate REPLICAOF command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.replicaof("host", 6379)).thenReturn(mockRedisFuture)

      testClass.replicaOf("host", 6379).map { _ =>
        verify(lettuceAsyncCommands).replicaof("host", 6379)
        succeed
      }
    }

    "delegate REPLICAOF NO ONE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.replicaofNoOne()).thenReturn(mockRedisFuture)

      testClass.replicaOfNoOne.map { _ =>
        verify(lettuceAsyncCommands).replicaofNoOne()
        succeed
      }
    }

    "delegate SLAVEOF command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.slaveof("host", 6379)).thenReturn(mockRedisFuture)

      testClass.slaveOf("host", 6379).map { _ =>
        verify(lettuceAsyncCommands).slaveof("host", 6379)
        succeed
      }
    }

    "delegate SLAVEOF NO ONE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.slaveofNoOne()).thenReturn(mockRedisFuture)

      testClass.slaveOfNoOne.map { _ =>
        verify(lettuceAsyncCommands).slaveofNoOne()
        succeed
      }
    }

    "delegate SAVE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.save()).thenReturn(mockRedisFuture)

      testClass.save.map { _ =>
        verify(lettuceAsyncCommands).save()
        succeed
      }
    }


  }


}

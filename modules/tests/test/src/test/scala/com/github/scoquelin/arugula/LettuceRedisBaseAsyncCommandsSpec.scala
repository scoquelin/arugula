package com.github.scoquelin.arugula

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands
import io.lettuce.core.RedisFuture
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}


class LettuceRedisBaseAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisBaseAsyncCommands" should {

    "delegate ECHO command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = "Hello, world!"
      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.echo(expectedValue)).thenReturn(mockRedisFuture)

      testClass.echo(expectedValue).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).echo(expectedValue)
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

    "delegate PUBLISH command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.publish("channel", "message")).thenReturn(mockRedisFuture)

      testClass.publish("channel", "message").map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).publish("channel", "message")
        succeed
      }
    }

    "delegate PUBSUB CHANNELS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("channel1", "channel2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.pubsubChannels()).thenReturn(mockRedisFuture)

      testClass.pubsubChannels().map { result =>
        result mustBe expectedValue.asScala.toList
        verify(lettuceAsyncCommands).pubsubChannels()
        succeed
      }
    }

    "delegate PUBSUB CHANNELS with pattern command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("channel1", "channel2")
      val mockRedisFuture: RedisFuture[java.util.List[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.pubsubChannels("chan*")).thenReturn(mockRedisFuture)

      testClass.pubsubChannels("chan*").map { result =>
        result mustBe expectedValue.asScala.toList
        verify(lettuceAsyncCommands).pubsubChannels("chan*")
        succeed
      }
    }

    "delegate PUBSUB NUMSUB command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue: java.util.Map[String, java.lang.Long] = new java.util.HashMap[String, java.lang.Long]()
      expectedValue.put("channel1", 1L)
      expectedValue.put("channel2", 2L)
      val mockRedisFuture: RedisFuture[java.util.Map[String, java.lang.Long]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.pubsubNumsub("channel1", "channel2")).thenReturn(mockRedisFuture)

      testClass.pubsubNumsub("channel1", "channel2").map { result =>
        result mustBe expectedValue.asScala.map { case (k, v) => (k, Long2long(v)) }
        verify(lettuceAsyncCommands).pubsubNumsub("channel1", "channel2")
        succeed
      }
    }

    "delegate PUBSUB NUMPAT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.pubsubNumpat()).thenReturn(mockRedisFuture)

      testClass.pubsubNumpat().map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).pubsubNumpat()
        succeed
      }
    }

    "delegate QUIT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.quit()).thenReturn(mockRedisFuture)

      testClass.quit().map { result =>
        verify(lettuceAsyncCommands).quit()
        succeed
      }
    }

    "delegate READONLY command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.readOnly()).thenReturn(mockRedisFuture)

      testClass.readOnly().map { result =>
        verify(lettuceAsyncCommands).readOnly()
        succeed
      }
    }

    "delegate READWRITE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val mockRedisFuture: RedisFuture[String] = mockRedisFutureToReturn("OK")
      when(lettuceAsyncCommands.readWrite()).thenReturn(mockRedisFuture)

      testClass.readWrite().map { result =>
        verify(lettuceAsyncCommands).readWrite()
        succeed
      }
    }

    "delegate ROLE command to Lettuce and lift master result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("master", 3456789L, java.util.List.of(java.util.List.of("host", "1234", "0")))
      val mockRedisFuture = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.role()).thenReturn(mockRedisFuture)

      testClass.role().map { result =>
        result mustBe RedisBaseAsyncCommands.Role.Master(3456789, List(RedisBaseAsyncCommands.Replica("host", 1234, 0)))
        verify(lettuceAsyncCommands).role()
        succeed
      }
    }

    "delegate ROLE command to Lettuce and lift slave result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("slave", "masterIp", 1234, "1234567", "connected", 1234567)
      val mockRedisFuture = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.role()).thenReturn(mockRedisFuture)

      testClass.role().map { result =>
        result mustBe RedisBaseAsyncCommands.Role.Slave("masterIp", 1234, 1234567, RedisBaseAsyncCommands.LinkStatus.Connected, 1234567)
        verify(lettuceAsyncCommands).role()
        succeed
      }
    }

    "delegate ROLE command to Lettuce and lift sentinel result into a Future" in { testContext =>
      import testContext._

      val expectedValue = java.util.List.of("sentinel", java.util.List.of("master1", "master2"))
      val mockRedisFuture = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.role()).thenReturn(mockRedisFuture)

      testClass.role().map { result =>
        result mustBe RedisBaseAsyncCommands.Role.Sentinel(List("master1", "master2"))
        verify(lettuceAsyncCommands).role()
        succeed
      }
    }

    "delegate WAIT command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._

      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.waitForReplication(1, java.time.Duration.ofSeconds(1).toMillis)).thenReturn(mockRedisFuture)

      testClass.waitForReplication(1, 1.second).map { result =>
        result mustBe expectedValue
        verify(lettuceAsyncCommands).waitForReplication(1, 1000)
        succeed
      }
    }
  }
}

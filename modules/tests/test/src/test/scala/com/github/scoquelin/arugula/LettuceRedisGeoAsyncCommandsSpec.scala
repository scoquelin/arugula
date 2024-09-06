package com.github.scoquelin.arugula

import com.github.scoquelin.arugula.commands.RedisGeoAsyncCommands
import io.lettuce.core.RedisFuture
import org.mockito.ArgumentMatchers.{any, anyString, eq => meq}
import org.mockito.Mockito.{verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatest.{FutureOutcome, wordspec}

class LettuceRedisGeoAsyncCommandsSpec extends wordspec.FixtureAsyncWordSpec with Matchers {

  override type FixtureParam = LettuceRedisCommandsClientFixture.TestContext

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    withFixture(test.toNoArgAsyncTest(new LettuceRedisCommandsClientFixture.TestContext))

  "LettuceRedisGeoAsyncCommands" should {
    "delegate GEOADD command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geoadd(anyString, any[Double], any[Double], anyString)).thenReturn(mockRedisFuture)
      testClass.geoAdd("Sicily", 13.361389, 38.115556, "Palermo").map { result =>
        result mustBe 1
        verify(lettuceAsyncCommands).geoadd("Sicily", 13.361389, 38.115556, "Palermo")
        succeed
      }
    }

    "delegate GEOADD command to Lettuce with args and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geoadd(anyString, any[Double], any[Double], anyString, any)).thenReturn(mockRedisFuture)
        .thenReturn(mockRedisFuture)
      testClass.geoAdd("Sicily", 13.361389, 38.115556, "Palermo", RedisGeoAsyncCommands.GeoAddArgs(nx = true)).map { result =>
        result mustBe 1
        verify(lettuceAsyncCommands).geoadd(meq("Sicily"), meq(13.361389), meq(38.115556), meq("Palermo"), any[io.lettuce.core.GeoAddArgs])
        succeed
      }
    }

    "delegate GEOADD command to Lettuce using GeoValue and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geoadd(anyString, any[io.lettuce.core.GeoValue[String]])).thenReturn(mockRedisFuture)
      testClass.geoAdd("Sicily", RedisGeoAsyncCommands.GeoValue("Palermo", 13.361389, 38.115556)).map { result =>
        result mustBe 1
        verify(lettuceAsyncCommands).geoadd("Sicily", io.lettuce.core.GeoValue.just( 13.361389, 38.115556, "Palermo"))
        succeed
      }
    }

    "delegate GEOADD command to Lettuce using GeoValue with args and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = 1L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geoadd(anyString, any[io.lettuce.core.GeoAddArgs], any[io.lettuce.core.GeoValue[String]])).thenReturn(mockRedisFuture)
      testClass.geoAdd("Sicily", RedisGeoAsyncCommands.GeoAddArgs(nx = true), RedisGeoAsyncCommands.GeoValue("Palermo", 13.361389, 38.115556)).map { result =>
        result mustBe 1
        verify(lettuceAsyncCommands).geoadd(meq("Sicily"), any[io.lettuce.core.GeoAddArgs], meq(io.lettuce.core.GeoValue.just(13.361389, 38.115556, "Palermo")))
        succeed
      }
    }

    "delegate GEODIST command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = 166274.1516
      val mockRedisFuture: RedisFuture[java.lang.Double] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geodist(anyString, anyString, anyString, any[io.lettuce.core.GeoArgs.Unit])).thenReturn(mockRedisFuture)
      testClass.geoDist("Sicily", "Palermo", "Catania", RedisGeoAsyncCommands.GeoUnit.Meters).map { result =>
        result mustBe Some(166274.1516)
        verify(lettuceAsyncCommands).geodist("Sicily", "Palermo", "Catania", io.lettuce.core.GeoArgs.Unit.m)
        succeed
      }
    }

    "delegate GEOHASH command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.List.of(io.lettuce.core.Value.just("sqc8b49rny0"))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.Value[String]]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geohash(anyString, anyString)).thenReturn(mockRedisFuture)
      testClass.geoHash("Sicily", "Palermo").map { result =>
        result mustBe Some("sqc8b49rny0")
        verify(lettuceAsyncCommands).geohash("Sicily", "Palermo")
        succeed
      }
    }

    "delegate GEOHASH command with multiple values to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.List.of(io.lettuce.core.Value.just("sqc8b49rny0"), io.lettuce.core.Value.just("sqc8b49rny1"))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.Value[String]]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geohash(anyString, anyString, anyString)).thenReturn(mockRedisFuture)
      testClass.geoHash("Sicily", List("Palermo", "Catania")).map { result =>
        result mustBe List(Some("sqc8b49rny0"), Some("sqc8b49rny1"))
        verify(lettuceAsyncCommands).geohash("Sicily", "Palermo", "Catania")
        succeed
      }
    }

    "delegate GEOPOS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.List.of(io.lettuce.core.GeoCoordinates.create(13.361389, 38.115556))
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.GeoCoordinates]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geopos(anyString, anyString)).thenReturn(mockRedisFuture)
      testClass.geoPos("Sicily", "Palermo").map { result =>
        result mustBe Some(RedisGeoAsyncCommands.GeoCoordinates(13.361389, 38.115556))
        verify(lettuceAsyncCommands).geopos("Sicily", "Palermo")
        succeed
      }
    }

    "delegate GEOPOS command with multiple members to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.List.of(
        io.lettuce.core.GeoCoordinates.create(13.361389, 38.115556),
        io.lettuce.core.GeoCoordinates.create(15.087269, 37.502669)
      )
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.GeoCoordinates]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geopos(anyString, anyString, anyString)).thenReturn(mockRedisFuture)
      testClass.geoPos("Sicily", List("Palermo", "Catania")).map { result =>
        result mustBe List(Some(RedisGeoAsyncCommands.GeoCoordinates(13.361389, 38.115556)), Some(RedisGeoAsyncCommands.GeoCoordinates(15.087269, 37.502669)))
        verify(lettuceAsyncCommands).geopos("Sicily", "Palermo", "Catania")
        succeed
      }
    }

    "delegate GEORADIUS command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.Set.of("Palermo", "Catania")
      val mockRedisFuture: RedisFuture[java.util.Set[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.georadius(anyString, any[Double], any[Double], any[Double], any[io.lettuce.core.GeoArgs.Unit])).thenReturn(mockRedisFuture)
      testClass.geoRadius("Sicily", 15, 37, 200, RedisGeoAsyncCommands.GeoUnit.Kilometers).map { result =>
        result mustBe Set("Palermo", "Catania")
        verify(lettuceAsyncCommands).georadius("Sicily", 15, 37, 200, io.lettuce.core.GeoArgs.Unit.km)
        succeed
      }
    }

    "delegate GEORADIUS command to Lettuce with args and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.List.of(
        new io.lettuce.core.GeoWithin("Palermo", 190.4424, 0L, io.lettuce.core.GeoCoordinates.create(13.361389, 38.115556)),
        new io.lettuce.core.GeoWithin("Catania", 56.4413, 0L, io.lettuce.core.GeoCoordinates.create(15.087269, 37.502669))
      )
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.GeoWithin[String]]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.georadius(anyString, any[Double], any[Double], any[Double], any[io.lettuce.core.GeoArgs.Unit], any[io.lettuce.core.GeoArgs])).thenReturn(mockRedisFuture)
      testClass.geoRadius("Sicily", 15.0, 37.0, 200.0, RedisGeoAsyncCommands.GeoUnit.Kilometers, RedisGeoAsyncCommands.GeoArgs(
        withDistance = true, withHash = true, withCoordinates = true, count = Some(2), sort = Some(RedisGeoAsyncCommands.GeoArgs.Sort.Asc)
      )).map { result =>
        result mustBe List(
          RedisGeoAsyncCommands.GeoWithin("Palermo", Some(190.4424), Some(0L), Some(RedisGeoAsyncCommands.GeoCoordinates(13.361389, 38.115556))),
          RedisGeoAsyncCommands.GeoWithin("Catania", Some(56.4413), Some(0L), Some(RedisGeoAsyncCommands.GeoCoordinates(15.087269, 37.502669))),
        )
        verify(lettuceAsyncCommands).georadius(meq("Sicily"), meq(15.0), meq(37.0), meq(200.0), meq(io.lettuce.core.GeoArgs.Unit.km), any[io.lettuce.core.GeoArgs])
        succeed
      }
    }

    "delegate GEORADIUS command to Lettuce with store args and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.georadius(anyString, any[Double], any[Double], any[Double], any[io.lettuce.core.GeoArgs.Unit], any[io.lettuce.core.GeoRadiusStoreArgs[String]])).thenReturn(mockRedisFuture)
      testClass.geoRadius("Sicily", 15.0, 37.0, 200.0, RedisGeoAsyncCommands.GeoUnit.Kilometers, RedisGeoAsyncCommands.GeoRadiusStoreArgs("storeKey", Some(2))).map { result =>
        result mustBe 2
        verify(lettuceAsyncCommands).georadius(meq("Sicily"), meq(15.0), meq(37.0), meq(200.0), meq(io.lettuce.core.GeoArgs.Unit.km), any[io.lettuce.core.GeoRadiusStoreArgs[String]])
        succeed
      }
    }

    "delegate GEORADIUSBYMEMBER command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.Set.of("Palermo", "Catania")
      val mockRedisFuture: RedisFuture[java.util.Set[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.georadiusbymember(anyString, anyString, any[Double], any[io.lettuce.core.GeoArgs.Unit])).thenReturn(mockRedisFuture)
      testClass.geoRadiusByMember("Sicily", "Palermo", 200, RedisGeoAsyncCommands.GeoUnit.Kilometers).map { result =>
        result mustBe Set("Palermo", "Catania")
        verify(lettuceAsyncCommands).georadiusbymember("Sicily", "Palermo", 200, io.lettuce.core.GeoArgs.Unit.km)
        succeed
      }
    }

    "delegate GEORADIUSBYMEMBER command to Lettuce with args and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.List.of(
        new io.lettuce.core.GeoWithin("Palermo", 190.4424, 0L, io.lettuce.core.GeoCoordinates.create(13.361389, 38.115556)),
        new io.lettuce.core.GeoWithin("Catania", 56.4413, 0L, io.lettuce.core.GeoCoordinates.create(15.087269, 37.502669))
      )
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.GeoWithin[String]]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.georadiusbymember(anyString, anyString, any[Double], any[io.lettuce.core.GeoArgs.Unit], any[io.lettuce.core.GeoArgs])).thenReturn(mockRedisFuture)
      testClass.geoRadiusByMember("Sicily", "Palermo", 200.0, RedisGeoAsyncCommands.GeoUnit.Kilometers, RedisGeoAsyncCommands.GeoArgs(
        withDistance = true, withHash = true, withCoordinates = true, count = Some(2), sort = Some(RedisGeoAsyncCommands.GeoArgs.Sort.Asc)
      )).map { result =>
        result mustBe List(
          RedisGeoAsyncCommands.GeoWithin("Palermo", Some(190.4424), Some(0L), Some(RedisGeoAsyncCommands.GeoCoordinates(13.361389, 38.115556))),
          RedisGeoAsyncCommands.GeoWithin("Catania", Some(56.4413), Some(0L), Some(RedisGeoAsyncCommands.GeoCoordinates(15.087269, 37.502669))),
        )
        verify(lettuceAsyncCommands).georadiusbymember(meq("Sicily"), meq("Palermo"), meq(200.0), meq(io.lettuce.core.GeoArgs.Unit.km), any[io.lettuce.core.GeoArgs])
        succeed
      }
    }

    "delegate GEOSEARCH command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.Set.of("Palermo", "Catania")
      val mockRedisFuture: RedisFuture[java.util.Set[String]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geosearch(anyString, any[io.lettuce.core.GeoSearch.GeoRef[String]], any[io.lettuce.core.GeoSearch.GeoPredicate])).thenReturn(mockRedisFuture)
      testClass.geoSearch("Sicily", RedisGeoAsyncCommands.GeoReference.FromMember("Palermo"), RedisGeoAsyncCommands.GeoPredicate.Radius(200.0, RedisGeoAsyncCommands.GeoUnit.Kilometers)).map { result =>
        result mustBe Set("Palermo", "Catania")
        verify(lettuceAsyncCommands).geosearch(meq("Sicily"), any[io.lettuce.core.GeoSearch.GeoRef[String]], any[io.lettuce.core.GeoSearch.GeoPredicate])
        succeed
      }
    }

    "delegate GEOSEARCH command to Lettuce with args and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = java.util.List.of(
        new io.lettuce.core.GeoWithin("Palermo", 190.4424, 0L, io.lettuce.core.GeoCoordinates.create(13.361389, 38.115556)),
        new io.lettuce.core.GeoWithin("Catania", 56.4413, 1L, io.lettuce.core.GeoCoordinates.create(15.087269, 37.502669))
      )
      val mockRedisFuture: RedisFuture[java.util.List[io.lettuce.core.GeoWithin[String]]] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geosearch(anyString, any[io.lettuce.core.GeoSearch.GeoRef[String]], any[io.lettuce.core.GeoSearch.GeoPredicate], any[io.lettuce.core.GeoArgs])).thenReturn(mockRedisFuture)
      testClass.geoSearch("Sicily", RedisGeoAsyncCommands.GeoReference.FromMember("Palermo"), RedisGeoAsyncCommands.GeoPredicate.Radius(200.0, RedisGeoAsyncCommands.GeoUnit.Kilometers), RedisGeoAsyncCommands.GeoArgs(
        withDistance = true, withHash = true, withCoordinates = true, count = Some(2), sort = Some(RedisGeoAsyncCommands.GeoArgs.Sort.Asc)
      )).map { result =>
        result mustBe List(
          RedisGeoAsyncCommands.GeoWithin("Palermo", Some(190.4424), Some(0L), Some(RedisGeoAsyncCommands.GeoCoordinates(13.361389, 38.115556))),
          RedisGeoAsyncCommands.GeoWithin("Catania", Some(56.4413), Some(1L), Some(RedisGeoAsyncCommands.GeoCoordinates(15.087269, 37.502669))),
        )
        verify(lettuceAsyncCommands).geosearch(meq("Sicily"), any[io.lettuce.core.GeoSearch.GeoRef[String]], any[io.lettuce.core.GeoSearch.GeoPredicate], any[io.lettuce.core.GeoArgs])
        succeed
      }
    }

    "delegate GEOSEARCHSTORE command to Lettuce and lift result into a Future" in { testContext =>
      import testContext._
      val expectedValue = 2L
      val mockRedisFuture: RedisFuture[java.lang.Long] = mockRedisFutureToReturn(expectedValue)
      when(lettuceAsyncCommands.geosearchstore(anyString, anyString, any[io.lettuce.core.GeoSearch.GeoRef[String]], any[io.lettuce.core.GeoSearch.GeoPredicate], any[io.lettuce.core.GeoArgs], any[java.lang.Boolean])).thenReturn(mockRedisFuture)
      testClass.geoSearchStore("destKey", "Sicily", RedisGeoAsyncCommands.GeoReference.FromMember("Palermo"), RedisGeoAsyncCommands.GeoPredicate.Radius(200.0, RedisGeoAsyncCommands.GeoUnit.Kilometers), RedisGeoAsyncCommands.GeoArgs(count = Some(2))).map { result =>
        result mustBe 2
        verify(lettuceAsyncCommands).geosearchstore(meq("destKey"), meq("Sicily"), any[io.lettuce.core.GeoSearch.GeoRef[String]], any[io.lettuce.core.GeoSearch.GeoPredicate], any[io.lettuce.core.GeoArgs], any[java.lang.Boolean])
        succeed
      }
    }
  }

}

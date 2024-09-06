package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.jdk.CollectionConverters.CollectionHasAsScala

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

trait LettuceRedisGeoAsyncCommands[K, V] extends RedisGeoAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V]{
  override def geoAdd(key: K, longitude: Double, latitude: Double, member: V): Future[Long] =
    delegateRedisClusterCommandAndLift(_.geoadd(key, longitude, latitude, member)).map(Long2long)

  override def geoAdd(key: K, longitude: Double, latitude: Double, member: V, args: RedisGeoAsyncCommands.GeoAddArgs): Future[Long] ={
    delegateRedisClusterCommandAndLift(_.geoadd(key, longitude, latitude, member, LettuceRedisGeoAsyncCommands.geoAddArgsToJava(args))).map(Long2long)
  }

  override def geoAdd(key: K, values: RedisGeoAsyncCommands.GeoValue[V]*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.geoadd(key, values.map(LettuceRedisGeoAsyncCommands.geoValueToJava): _*)).map(Long2long)

  override def geoAdd(key: K, args: RedisGeoAsyncCommands.GeoAddArgs, values: RedisGeoAsyncCommands.GeoValue[V]*): Future[Long] =
    delegateRedisClusterCommandAndLift(_.geoadd(key, LettuceRedisGeoAsyncCommands.geoAddArgsToJava(args), values.map(LettuceRedisGeoAsyncCommands.geoValueToJava): _*)).map(Long2long)

  override def geoDist(key: K, from: V, to: V, unit: RedisGeoAsyncCommands.GeoUnit): Future[Option[Double]] =
    delegateRedisClusterCommandAndLift(_.geodist(key, from, to, LettuceRedisGeoAsyncCommands.geoUnitToJava(unit))).map{
      case null => None
      case d => Some(Double2double(d))
    }

  override def geoHash(key: K, member: V): Future[Option[String]] =
    delegateRedisClusterCommandAndLift(_.geohash(key, member)).map(_.asScala.headOption.map{
      case value if value.hasValue => value.getValue
      case _ => null
    })

  override def geoHash(key: K, members: List[V]): Future[List[Option[String]]] =
    delegateRedisClusterCommandAndLift(_.geohash(key, members: _*)).map(_.asScala.toList.map{
      case value if value.hasValue => Some(value.getValue)
      case _ => None
    })

  override def geoPos(key: K, member: V): Future[Option[RedisGeoAsyncCommands.GeoCoordinates]] =
    delegateRedisClusterCommandAndLift(_.geopos(key, member)).map(_.asScala.headOption.map{ coordinates =>
      RedisGeoAsyncCommands.GeoCoordinates(coordinates.getX.doubleValue(), coordinates.getY.doubleValue())
    })

  override def geoPos(key: K, members: List[V]): Future[List[Option[RedisGeoAsyncCommands.GeoCoordinates]]] =
    delegateRedisClusterCommandAndLift(_.geopos(key, members: _*)).map(_.asScala.toList.map{
      case null => None
      case coordinates => Some(RedisGeoAsyncCommands.GeoCoordinates(coordinates.getX.doubleValue(), coordinates.getY.doubleValue()))
    })

  override def geoRadius(key: K,
    longitude: Double,
    latitude: Double,
    radius: Double,
    unit: RedisGeoAsyncCommands.GeoUnit): Future[Set[V]] = {
    delegateRedisClusterCommandAndLift(_.georadius(key, longitude, latitude, radius, LettuceRedisGeoAsyncCommands.geoUnitToJava(unit))).map(_.asScala.toSet)
  }

  override def geoRadius(key: K,
    longitude: Double,
    latitude: Double,
    radius: Double,
    unit: RedisGeoAsyncCommands.GeoUnit,
    args: RedisGeoAsyncCommands.GeoArgs): Future[List[RedisGeoAsyncCommands.GeoWithin[V]]] =
      delegateRedisClusterCommandAndLift(_.georadius(key, longitude, latitude, radius, LettuceRedisGeoAsyncCommands.geoUnitToJava(unit), LettuceRedisGeoAsyncCommands.geoArgsToJava(args)))
        .map(_.asScala.toList.flatMap(LettuceRedisGeoAsyncCommands.javaToGeoWithin))

  override def geoRadius(key: K,
    longitude: Double,
    latitude: Double,
    radius: Double,
    unit: RedisGeoAsyncCommands.GeoUnit,
    storeArgs: RedisGeoAsyncCommands.GeoRadiusStoreArgs[K]): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.georadius(key, longitude, latitude, radius, LettuceRedisGeoAsyncCommands.geoUnitToJava(unit), LettuceRedisGeoAsyncCommands.geoStoreArgsToJava(storeArgs))).map(Long2long)
  }

  override def geoRadiusByMember(key: K, member: V, radius: Double, unit: RedisGeoAsyncCommands.GeoUnit): Future[Set[V]] = {
    delegateRedisClusterCommandAndLift(_.georadiusbymember(key, member, radius, LettuceRedisGeoAsyncCommands.geoUnitToJava(unit))).map(_.asScala.toSet)
  }

  override def geoRadiusByMember(key: K,
    member: V,
    radius: Double,
    unit: RedisGeoAsyncCommands.GeoUnit,
    args: RedisGeoAsyncCommands.GeoArgs): Future[List[RedisGeoAsyncCommands.GeoWithin[V]]] =
      delegateRedisClusterCommandAndLift(_.georadiusbymember(key, member, radius, LettuceRedisGeoAsyncCommands.geoUnitToJava(unit), LettuceRedisGeoAsyncCommands.geoArgsToJava(args)))
        .map(_.asScala.toList.flatMap(LettuceRedisGeoAsyncCommands.javaToGeoWithin))

  override def geoRadiusByMember(key: K, member: V, radius: Double, unit: RedisGeoAsyncCommands.GeoUnit, storeArgs: RedisGeoAsyncCommands.GeoRadiusStoreArgs[K]): Future[Long] =
    delegateRedisClusterCommandAndLift(_.georadiusbymember(key, member, radius, LettuceRedisGeoAsyncCommands.geoUnitToJava(unit), LettuceRedisGeoAsyncCommands.geoStoreArgsToJava(storeArgs))).map(Long2long)

  override def geoSearch(key: K, reference: RedisGeoAsyncCommands.GeoReference[K], predicate: RedisGeoAsyncCommands.GeoPredicate): Future[Set[V]] = {
    delegateRedisClusterCommandAndLift(_.geosearch(key, LettuceRedisGeoAsyncCommands.geoReferenceToJava(reference), LettuceRedisGeoAsyncCommands.geoPredicateToJava(predicate))).map(_.asScala.toSet)
  }

  override def geoSearch(
    key: K,
    reference: RedisGeoAsyncCommands.GeoReference[K],
    predicate: RedisGeoAsyncCommands.GeoPredicate,
    args: RedisGeoAsyncCommands.GeoArgs): Future[List[RedisGeoAsyncCommands.GeoWithin[V]]] = {
    delegateRedisClusterCommandAndLift(_.geosearch(key, LettuceRedisGeoAsyncCommands.geoReferenceToJava(reference), LettuceRedisGeoAsyncCommands.geoPredicateToJava(predicate), LettuceRedisGeoAsyncCommands.geoArgsToJava(args)))
      .map(_.asScala.toList.flatMap(LettuceRedisGeoAsyncCommands.javaToGeoWithin))
  }

  override def geoSearchStore(
    destination: K,
    key: K,
    reference: RedisGeoAsyncCommands.GeoReference[K],
    predicate: RedisGeoAsyncCommands.GeoPredicate,
    args: RedisGeoAsyncCommands.GeoArgs,
    storeDist: Boolean = false
  ): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.geosearchstore(destination, key, LettuceRedisGeoAsyncCommands.geoReferenceToJava(reference), LettuceRedisGeoAsyncCommands.geoPredicateToJava(predicate), LettuceRedisGeoAsyncCommands.geoArgsToJava(args), storeDist)).map(Long2long)
  }

}

private[arugula] object LettuceRedisGeoAsyncCommands {

  private[arugula] def geoAddArgsToJava(args: RedisGeoAsyncCommands.GeoAddArgs): io.lettuce.core.GeoAddArgs = {
    val options = new io.lettuce.core.GeoAddArgs()
    if (args.xx) options.xx()
    if (args.nx) options.nx()
    if (args.ch) options.ch()
    options
  }

  private[arugula] def geoValueToJava[V](value: RedisGeoAsyncCommands.GeoValue[V]): io.lettuce.core.GeoValue[V] = {
    io.lettuce.core.GeoValue.just(new io.lettuce.core.GeoCoordinates(value.position.longitude, value.position.latitude), value.member)
  }

  private[arugula] def geoUnitToJava(unit: RedisGeoAsyncCommands.GeoUnit): io.lettuce.core.GeoArgs.Unit = unit match {
    case RedisGeoAsyncCommands.GeoUnit.Meters => io.lettuce.core.GeoArgs.Unit.m
    case RedisGeoAsyncCommands.GeoUnit.Kilometers => io.lettuce.core.GeoArgs.Unit.km
    case RedisGeoAsyncCommands.GeoUnit.Miles => io.lettuce.core.GeoArgs.Unit.mi
    case RedisGeoAsyncCommands.GeoUnit.Feet => io.lettuce.core.GeoArgs.Unit.ft
  }

  private[arugula] def geoArgsToJava(args: RedisGeoAsyncCommands.GeoArgs): io.lettuce.core.GeoArgs = {
    val options = new io.lettuce.core.GeoArgs()
    if (args.withDistance) options.withDistance()
    if(args.withHash) options.withHash()
    if(args.withCoordinates) options.withCoordinates()
    args.count.foreach(options.withCount(_, args.any))
    args.sort.foreach{
      case RedisGeoAsyncCommands.GeoArgs.Sort.Asc => options.sort(io.lettuce.core.GeoArgs.Sort.asc)
      case RedisGeoAsyncCommands.GeoArgs.Sort.Desc => options.sort(io.lettuce.core.GeoArgs.Sort.desc)
    }
    options
  }

  private[arugula] def geoStoreArgsToJava[K](args: RedisGeoAsyncCommands.GeoRadiusStoreArgs[K]): io.lettuce.core.GeoRadiusStoreArgs[K] = {
    val options: io.lettuce.core.GeoRadiusStoreArgs[K] = new io.lettuce.core.GeoRadiusStoreArgs[K]()
    args.storageType match {
      case RedisGeoAsyncCommands.GeoRadiusStoreArgs.StorageType.Store => options.withStore(args.key)
      case RedisGeoAsyncCommands.GeoRadiusStoreArgs.StorageType.StoreDist => options.withStoreDist(args.key)
    }
    args.count.foreach(options.withCount)
    args.sort.foreach{
      case RedisGeoAsyncCommands.GeoArgs.Sort.Asc => options.sort(io.lettuce.core.GeoArgs.Sort.asc)
      case RedisGeoAsyncCommands.GeoArgs.Sort.Desc => options.sort(io.lettuce.core.GeoArgs.Sort.desc)
    }
    options
  }

  private[arugula] def javaToGeoWithin[V](value: io.lettuce.core.GeoWithin[V]): Option[RedisGeoAsyncCommands.GeoWithin[V]] = {
    value match {
      case null => None
      case _ if value.getMember == null => None
      case _ => Some(RedisGeoAsyncCommands.GeoWithin(
        member = value.getMember,
        distance = Option(value.getDistance).map(Double2double),
        geohash = Option(value.getGeohash).map(Long2long),
        coordinates = Option(value.getCoordinates).map { coordinates => RedisGeoAsyncCommands.GeoCoordinates(coordinates.getX.doubleValue(), coordinates.getY.doubleValue()) }
      ))
    }
  }

  private[arugula] def geoReferenceToJava[K](ref: RedisGeoAsyncCommands.GeoReference[K]): io.lettuce.core.GeoSearch.GeoRef[K] = {
    ref match {
      case RedisGeoAsyncCommands.GeoReference.FromMember(member) => io.lettuce.core.GeoSearch.fromMember(member)
      case RedisGeoAsyncCommands.GeoReference.FromCoordinates(longitude, latitude) => io.lettuce.core.GeoSearch.fromCoordinates(longitude, latitude)
    }
  }

  private[arugula] def geoPredicateToJava(predicate: RedisGeoAsyncCommands.GeoPredicate): io.lettuce.core.GeoSearch.GeoPredicate = {
    predicate match {
      case RedisGeoAsyncCommands.GeoPredicate.Radius(radius, unit) => io.lettuce.core.GeoSearch.byRadius(radius, geoUnitToJava(unit))
      case RedisGeoAsyncCommands.GeoPredicate.Box(width, height, unit) => io.lettuce.core.GeoSearch.byBox(width, height, geoUnitToJava(unit))
    }
  }
}


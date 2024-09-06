package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

import com.github.scoquelin.arugula.commands.RedisGeoAsyncCommands.GeoPredicate

trait RedisGeoAsyncCommands[K, V] {
  /**
   * Adds the specified geospatial items (latitude, longitude, member) to the specified key.
   * @param key The key
   * @param longitude The longitude
   * @param latitude The latitude
   * @param member The member
   * @return The number of elements that were added to the set
   */
  def geoAdd(key: K, longitude: Double, latitude: Double, member: V): Future[Long]

  /**
   * Adds the specified geospatial items (latitude, longitude, member) to the specified key.
   * @param key The key
   * @param longitude The longitude
   * @param latitude The latitude
   * @param member The member
   * @param args The arguments
   * @return The number of elements that were added to the set
   */
  def geoAdd(key: K, longitude: Double, latitude: Double, member: V, args: RedisGeoAsyncCommands.GeoAddArgs): Future[Long]

  /**
   * Adds the specified geospatial items to the specified key.
   * @param key The key
   * @param values The values
   * @return The number of elements that were added to the set
   */
  def geoAdd(key: K, values: RedisGeoAsyncCommands.GeoValue[V]*): Future[Long]

  /**
   * Adds the specified geospatial items to the specified key.
   * @param key The key
   * @param args The arguments
   * @param values The values
   * @return The number of elements that were added to the set
   */
  def geoAdd(key: K, args: RedisGeoAsyncCommands.GeoAddArgs, values: RedisGeoAsyncCommands.GeoValue[V]*): Future[Long]

  /**
   * Returns the distance between two members in the geospatial index represented by the key.
   * @param key The key
   * @param from The from member
   * @param to The to member
   * @param unit The unit
   * @return The distance between the two members
   */
  def geoDist(key: K, from: V, to: V, unit: RedisGeoAsyncCommands.GeoUnit): Future[Option[Double]]

  /**
   * Retrieve Geohash strings representing the position of an element in a geospatial index.
   * @param key The key
   * @param member The member
   * @see https://redis.io/commands/geohash
   * @see https://en.wikipedia.org/wiki/Geohash
   * @return The position of the member in the geospatial index as a geo-hash string
   */
  def geoHash(key: K, member: V): Future[Option[String]]

  /**
   * Retrieve Geohash strings representing the position of elements in a geospatial index.
   * @param key The key
   * @param members The members
   * @see https://redis.io/commands/geohash
   * @see https://en.wikipedia.org/wiki/Geohash
   * @return The position of the members in the geospatial index as geo-hash strings.
   *         If a member does not exist in the geospatial index, the corresponding position will be None.
   */
  def geoHash(key: K, members: List[V]): Future[List[Option[String]]]

  /**
   * Retrieve the longitude and latitude of a member in a geospatial index.
   * @param key The key
   * @param member The member
   * @return An optional pair of longitude and latitude coordinates of the member in the geospatial index as a GeoCoordinates object.
   */
  def geoPos(key: K, member: V): Future[Option[RedisGeoAsyncCommands.GeoCoordinates]]

  /**
   * Retrieve the longitude and latitude of members in a geospatial index.
   * @param key The key
   * @param members The members
   * @return A list of optional pairs of longitude and latitude coordinates of the members in the geospatial index as GeoCoordinates objects.
   *         If a member does not exist in the geospatial index, the corresponding position will be None.
   */
  def geoPos(key: K, members: List[V]): Future[List[Option[RedisGeoAsyncCommands.GeoCoordinates]]]

  /**
   * Query a sorted set representing a geospatial index to fetch members matching a given circular area.
   * @param key The key
   * @param longitude The longitude
   * @param latitude The latitude
   * @param radius The radius
   * @param unit The unit
   * @return The members within the circular area
   */
  def geoRadius(key: K, longitude: Double, latitude: Double, radius: Double, unit: RedisGeoAsyncCommands.GeoUnit): Future[Set[V]]

  /**
   * Query a sorted set representing a geospatial index to fetch members matching a given circular area.
   * @param key The key
   * @param longitude The longitude
   * @param latitude The latitude
   * @param radius The radius
   * @param unit The unit
   * @param args The arguments
   * @return The members within the circular area
   */
  def geoRadius(key: K, longitude: Double, latitude: Double, radius: Double, unit: RedisGeoAsyncCommands.GeoUnit, args: RedisGeoAsyncCommands.GeoArgs): Future[List[RedisGeoAsyncCommands.GeoWithin[V]]]


  /**
   * Query a sorted set representing a geospatial index to fetch members matching a given circular area and store the result in a key.
   * @param key The key
   * @param longitude The longitude
   * @param latitude The latitude
   * @param radius The radius
   * @param unit The unit
   * @param storeArgs The store arguments
   * @return The number of elements that were added to the set
   */
  def geoRadius(key: K, longitude: Double, latitude: Double, radius: Double, unit: RedisGeoAsyncCommands.GeoUnit, storeArgs: RedisGeoAsyncCommands.GeoRadiusStoreArgs[K]): Future[Long]

  /**
   * Retrieve members selected by distance with the center of member.
   * The member itself is always contained in the results.
   * @param key The key
   * @param member The member
   * @param radius The radius
   * @param unit The unit of measurement (meters, kilometers, miles, feet)
   * @return The members within the circular area
   */
  def geoRadiusByMember(key: K, member: V, radius: Double, unit: RedisGeoAsyncCommands.GeoUnit): Future[Set[V]]

  /**
   * Retrieve members selected by distance with the center of member.
   * The member itself is always contained in the results.
   * @param key The key
   * @param member The member
   * @param radius The radius
   * @param unit The unit of measurement (meters, kilometers, miles, feet)
   * @param args The arguments to customize the results
   * @return The members within the circular area with additional information
   */
  def geoRadiusByMember(key: K, member: V, radius: Double, unit: RedisGeoAsyncCommands.GeoUnit, args: RedisGeoAsyncCommands.GeoArgs): Future[List[RedisGeoAsyncCommands.GeoWithin[V]]]


  /**
   * Query a sorted set representing a geospatial index to fetch members matching a given circular area and store the result in a key.
   * @param key The key
   * @param member The member
   * @param radius The radius
   * @param unit The unit of measurement (meters, kilometers, miles, feet)
   * @param storeArgs The store arguments
   * @return The number of elements that were added to the set
   */
  def geoRadiusByMember(key: K, member: V, radius: Double, unit: RedisGeoAsyncCommands.GeoUnit, storeArgs: RedisGeoAsyncCommands.GeoRadiusStoreArgs[K]): Future[Long]

  /**
   * Query a sorted set representing a geospatial index to fetch members matching a given circular area.
   * @param key The key
   * @param reference The reference (member or coordinates)
   * @param predicate The predicate (radius or box)
   * @return The members within the circular area
   */
  def geoSearch(key: K, reference: RedisGeoAsyncCommands.GeoReference[K], predicate: RedisGeoAsyncCommands.GeoPredicate): Future[Set[V]]

  /**
   * Query a sorted set representing a geospatial index to fetch members matching a given circular area.
   * @param key The key
   * @param reference The reference (member or coordinates)
   * @param predicate The predicate (radius or box)
   * @param args The arguments to customize the results
   * @return The members within the circular area with additional information
   */
  def geoSearch(key: K, reference: RedisGeoAsyncCommands.GeoReference[K], predicate: RedisGeoAsyncCommands.GeoPredicate, args: RedisGeoAsyncCommands.GeoArgs): Future[List[RedisGeoAsyncCommands.GeoWithin[V]]]

  /**
   * Query a sorted set representing a geospatial index to fetch members matching a given circular area and store the result in a key.
   * @param destination The destination key
   * @param key The key
   * @param reference The reference (member or coordinates)
   * @param predicate The predicate (radius or box)
   * @param args The arguments to customize the results
   * @param storeDist if enabled, stores the items in a sorted set populated with their distance from the center of the circle or box,
   *                  as a floating-point number, in the same unit specified for that shape.
   * @return
   */
  def geoSearchStore(destination: K, key: K, reference: RedisGeoAsyncCommands.GeoReference[K], predicate: RedisGeoAsyncCommands.GeoPredicate, args: RedisGeoAsyncCommands.GeoArgs, storeDist: Boolean = false): Future[Long]
}

object RedisGeoAsyncCommands {

  /**
   * Arguments for the geoAdd command
   * @param xx Only update elements that already exist
   * @param nx Only add new elements
   * @param ch Modify the return value from the number of new elements added, to the total number of elements changed
   */
  case class GeoAddArgs(
    xx: Boolean = false,
    nx: Boolean = false,
    ch: Boolean = false
  )

  /**
   * Represents a pair of longitude and latitude coordinates
   * @param longitude The longitude coordinate as a decimal value
   * @param latitude The latitude coordinate as a decimal value
   */
  case class GeoCoordinates(longitude: Double, latitude: Double)

  /**
   * Represents a geospatial value
   * @param member The member
   * @param position The position
   */
  case class GeoValue[V](member: V, position: GeoCoordinates)

  object GeoValue {
    /**
     * convenience method to create a GeoValue
     * @param member The member
     * @param longitude The longitude
     * @param latitude The latitude
     * @tparam V The type of the member
     * @return A GeoValue
     */
    def apply[V](member: V, longitude: Double, latitude: Double): GeoValue[V] = GeoValue(member, GeoCoordinates(longitude, latitude))
  }

  /**
   * Represents a geospatial unit of measurement
   */
  sealed trait GeoUnit

  object GeoUnit {
    /**
     * The unit of measurement in meters
     */
    case object Meters extends GeoUnit

    /**
     * The unit of measurement in kilometers
     */
    case object Kilometers extends GeoUnit

    /**
     * The unit of measurement in miles
     */
    case object Miles extends GeoUnit

    /**
     * The unit of measurement in feet
     */
    case object Feet extends GeoUnit
  }

  /**
   * Arguments for the geoRadius command
   * @param withDistance Include the distance of items from the center of the circle
   * @param withHash Include the geohash value of items
   * @param withCoordinates Include the longitude and latitude of items
   * @param count Limit the number of results
   * @param any Return the items in an arbitrary order
   * @param sort Sort the results
   */
  case class GeoArgs(
    withDistance: Boolean = false,
    withHash: Boolean = false,
    withCoordinates: Boolean = false,
    count: Option[Long] = None,
    any: Boolean = false,
    sort: Option[GeoArgs.Sort] = None
  )

  object GeoArgs {
    /**
     * Represents the sort order for the geoRadius command
     */
    sealed trait Sort

    object Sort{
      /**
       * Sort the results in ascending order
       */
      case object Asc extends Sort

      /**
       * Sort the results in descending order
       */
      case object Desc extends Sort
    }
  }

  /**
   * Represents the result of a geoRadius command
   * @param member The member
   * @param distance The distance from the center of the circle
   * @param geohash The geohash value as a high-resolution Long number
   * @param coordinates The longitude and latitude
   */
  case class GeoWithin[V](
    member: V,
    distance: Option[Double],
    geohash: Option[Long],
    coordinates: Option[GeoCoordinates]
  )

  /**
   * Arguments for the geoRadiusStore command
   * @param key The key to store the results
   * @param count Limit the number of results
   * @param sort Sort the results
   * @param storageType The storage type (Store or StoreDist)
   * @tparam K The type of the key
   */
  case class GeoRadiusStoreArgs[K](
    key: K,
    count: Option[Long] = None,
    sort: Option[GeoArgs.Sort] = None,
    storageType: GeoRadiusStoreArgs.StorageType = GeoRadiusStoreArgs.StorageType.Store
  )

  object GeoRadiusStoreArgs {
    /**
     * Represents the storage type for the geoRadiusStore command
     */
    sealed trait StorageType
    object StorageType {
      /**
       * Store the results (default)
       */
      case object Store extends StorageType

      /**
       * Store the results with their distance from the center of the circle
       */
      case object StoreDist extends StorageType
    }
  }

  /**
   * Represents a reference for the geoSearch command
   * @tparam K The type of the key
   */
  sealed trait GeoReference[K]

  object GeoReference{
    /**
     * Represents a reference from a member
     * @param member The member
     * @tparam K The type of the key
     */
    case class FromMember[K](member: K) extends GeoReference[K]

    /**
     * Represents a reference from coordinates
     * @param longitude The longitude
     * @param latitude The latitude
     */
    case class FromCoordinates(longitude: Double, latitude: Double) extends GeoReference[Nothing]
  }

  /**
   * Represents a predicate for the geoSearch command
   */
  sealed trait GeoPredicate

  object GeoPredicate{
    /**
     * Represents a radius predicate
     * @param radius The radius
     * @param unit The unit of measurement
     */
    case class Radius(radius: Double, unit: GeoUnit) extends GeoPredicate

    /**
     * Represents a box predicate
     * @param width The width
     * @param height The height
     * @param unit The unit of measurement
     */
    case class Box(width: Double, height: Double, unit: GeoUnit) extends GeoPredicate
  }
}
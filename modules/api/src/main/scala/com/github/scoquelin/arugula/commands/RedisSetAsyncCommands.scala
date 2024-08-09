package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

import com.github.scoquelin.arugula.commands.RedisBaseAsyncCommands.{InitialCursor, ScanResults}

/**
 * Asynchronous commands for manipulating/querying Sets
 * @tparam K The key type
 * @tparam V The value type
 */

trait RedisSetAsyncCommands[K, V] {
  /**
   * add one or more members to a set
   * @param key The key
   * @param values The values to add
   * @return The number of elements that were added to the set
   */
  def sAdd(key: K, values: V*): Future[Long]

  /**
   * get the number of members in a set
   * @param key The key
   * @return The number of elements in the set
   */
  def sCard(key: K): Future[Long]

  /**
   * get the difference between sets
   * @param keys The keys
   * @return The difference between the sets
   */
  def sDiff(keys: K*): Future[Set[V]]

  /**
   * store the difference between sets
   * @param destination The destination key
   * @param keys The keys
   * @return The number of elements in the resulting set
   */
  def sDiffStore(destination: K, keys: K*): Future[Long]

  /**
   * get the intersection between sets
   * @param keys The keys
   * @return The intersection between the sets
   */
  def sInter(keys: K*): Future[Set[V]]

  /**
   * get the number of elements in the intersection between sets
   * @param keys The keys
   * @return The number of elements in the resulting set
   */
  def sInterCard(keys: K*): Future[Long]

  /**
   * get the intersection between sets and store the result
   * @param destination The destination key
   * @param keys The keys
   * @return The number of elements in the resulting set
   */
  def sInterStore(destination: K, keys: K*): Future[Long]

  /**
   * determine if a member is in a set
   * @param key The key
   * @param value The value
   * @return True if the value is in the set, false otherwise
   */
  def sIsMember(key: K, value: V): Future[Boolean]

  /**
   * get all the members in a set
   * @param key The key
   * @return A list of all the members in the set
   */
  def sMembers(key: K): Future[Set[V]]

  /**
   * determine if members are in a set
   * @param key The key
   * @param values The values
   * @return A list of booleans indicating if the values are in the set
   */
  def smIsMember(key: K, values: V*): Future[List[Boolean]]

  /**
   * move a member from one set to another
   * @param source The source key
   * @param destination The destination key
   * @param member The member to move
   * @return True if the member was moved, false otherwise
   */
  def sMove(source: K, destination: K, member: V): Future[Boolean]

  /**
   * Remove and return a random member from a set.
   * @param key The key
   * @return The removed member, or None if the set is empty
   */
  def sPop(key: K): Future[Option[V]]

  /**
   * Get a random member from a set.
   * @param key The key
   * @return The random member, or None if the set is empty
   */
  def sRandMember(key: K): Future[Option[V]]

  /**
   * Get one or more random members from a set.
   * @param key The key
   * @param count The number of members to get
   * @return The random members
   */
  def sRandMember(key: K, count: Long): Future[Set[V]]

  /**
   * Remove one or more members from a set
   * @param key The key
   * @param values The values to remove
   * @return The number of members that were removed from the set
   */
  def sRem(key: K, values: V*): Future[Long]

  /**
   * Get the union between sets
   * @param keys The keys
   * @return The union between the sets
   */
  def sUnion(keys: K*): Future[Set[V]]

  /**
   * Store the union between sets
   * @param destination The destination key
   * @param keys The keys
   * @return The number of elements in the resulting set
   */
  def sUnionStore(destination: K, keys: K*): Future[Long]

  /**
   * Incrementally iterate over a set, retrieving members in batches, using a cursor
   * to resume from the last position
   * @param key The key
   * @param cursor The cursor
   * @param limit The maximum number of elements to return
   *              (note: the actual number of elements returned may be less than the limit)
   *              (note: if the limit is None, the server will determine the number of elements to return)
   * @param matchPattern The pattern to match
   *                     (note: the pattern is a glob-style pattern)
   *                     (note: if the pattern is None, all elements will be returned)
   * @return The cursor, whether the cursor is finished, and the elements
   */
  def sScan(
    key: K,
    cursor: String = InitialCursor,
    limit: Option[Long] = None,
    matchPattern: Option[String] = None
  ): Future[ScanResults[Set[V]]]
}

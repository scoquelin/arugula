package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

/**
 * This trait provides Redis HyperLogLog commands.
 * HyperLogLog is a probabilistic data structure used to estimate the cardinality of a set.
 * @see https://redis.io/docs/latest/develop/data-types/probabilistic/hyperloglogs/
 * @tparam K The key type (usually String)
 * @tparam V The value type (usually String)
 */
trait RedisHLLAsyncCommands[K, V] {
  /**
   * Adds the specified elements to the specified HyperLogLog
   * @param key The key of the HyperLogLog
   * @param values The values to add
   * @return The number of elements that were added to the HyperLogLog
   */
  def pfAdd(key: K, values: V*): Future[Long]

  /**
   * Merge multiple HyperLogLogs into a single one
   * @param destinationKey The key of the HyperLogLog to merge into
   * @param sourceKeys The keys of the HyperLogLogs to merge
   */
  def pfMerge(destinationKey: K, sourceKeys: K*): Future[Unit]

  /**
   * Get the number of elements in the HyperLogLog
   * @param keys The keys of the HyperLogLogs to count
   * @return The number of elements in the HyperLogLog
   */
  def pfCount(keys: K*): Future[Long]
}

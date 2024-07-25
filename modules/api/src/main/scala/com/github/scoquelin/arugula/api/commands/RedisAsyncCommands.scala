package com.github.scoquelin.arugula.api.commands

/**
 * This trait is aggregating all the asynchronous commands that need to be supported by the API
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisAsyncCommands[K, V]
  extends RedisBaseAsyncCommands[K, V]
  with RedisKeyAsyncCommands[K, V]
  with RedisStringAsyncCommands[K, V]
  with RedisHashAsyncCommands[K, V]
  with RedisListAsyncCommands[K, V]
  with RedisSortedSetAsyncCommands[K, V]
  with RedisServerAsyncCommands[K, V]
  with RedisPipelineAsyncCommands[K, V]
package com.github.scoquelin.arugula

import com.github.scoquelin.arugula.commands._

/**
 * This trait is aggregating all the asynchronous commands that need to be supported by the API
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisCommandsClient[K, V]
  extends RedisBaseAsyncCommands[K, V]
  with RedisKeyAsyncCommands[K, V]
  with RedisStringAsyncCommands[K, V]
  with RedisHashAsyncCommands[K, V]
  with RedisHLLAsyncCommands[K, V]
  with RedisListAsyncCommands[K, V]
  with RedisSetAsyncCommands[K, V]
  with RedisSortedSetAsyncCommands[K, V]
  with RedisScriptingAsyncCommands[K, V]
  with RedisServerAsyncCommands[K, V]
  with RedisPipelineAsyncCommands[K, V]
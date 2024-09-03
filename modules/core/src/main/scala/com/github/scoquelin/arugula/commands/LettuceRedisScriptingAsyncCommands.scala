package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import com.github.scoquelin.arugula.RedisCommandsClient
import com.github.scoquelin.arugula.commands.RedisScriptingAsyncCommands.ScriptOutputType
import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation


trait LettuceRedisScriptingAsyncCommands[K, V] extends RedisScriptingAsyncCommands[K, V]  with LettuceRedisCommandDelegation[K, V] { this: RedisCommandsClient[K, V] =>


  override def eval(script: String, outputType: ScriptOutputType, keys:K*): Future[outputType.R] = {
    delegateRedisClusterCommandAndLift(_.eval(
      script,
      LettuceRedisScriptingAsyncCommands.outputTypeToJava(outputType),
      keys:_*)).map(outputType.convert)
  }

  override def eval(script: Array[Byte], outputType: ScriptOutputType, keys: K*): Future[outputType.R] = {
    delegateRedisClusterCommandAndLift(_.eval(
      script,
      LettuceRedisScriptingAsyncCommands.outputTypeToJava(outputType),
      keys:_*)).map(outputType.convert)
  }

  override def eval(script: String, outputType: ScriptOutputType, keys: List[K], values: V*): Future[outputType.R] = {
    delegateRedisClusterCommandAndLift(_.eval(
      script,
      LettuceRedisScriptingAsyncCommands.outputTypeToJava(outputType),
      keys.toArray[Any].asInstanceOf[Array[K with AnyRef]],
      values:_*)).map(outputType.convert)
  }

override def eval(script: Array[Byte], outputType: ScriptOutputType, keys: List[K], values: V*): Future[outputType.R] = {
    delegateRedisClusterCommandAndLift(_.eval(
      script,
      LettuceRedisScriptingAsyncCommands.outputTypeToJava(outputType),
      keys.toArray[Any].asInstanceOf[Array[K with AnyRef]],
      values: _*)).map(outputType.convert)
  }

  override def evalSha(sha: String, outputType: ScriptOutputType, keys:K*): Future[outputType.R] = {
    delegateRedisClusterCommandAndLift(_.evalsha(
      sha,
      LettuceRedisScriptingAsyncCommands.outputTypeToJava(outputType),
      keys:_*)).map(outputType.convert)
  }

  override def evalSha(sha: String, outputType: ScriptOutputType, keys: List[K], values: V*): Future[outputType.R] = {
    delegateRedisClusterCommandAndLift(_.evalsha(
      sha,
      LettuceRedisScriptingAsyncCommands.outputTypeToJava(outputType),
      keys.toArray[Any].asInstanceOf[Array[K with AnyRef]],
      values:_*)).map(outputType.convert)
  }

  override def evalReadOnly(script: String, outputType: ScriptOutputType, keys:List[K], values: V*): Future[outputType.R] = {
    delegateRedisClusterCommandAndLift(_.evalReadOnly(
      script.getBytes,
      LettuceRedisScriptingAsyncCommands.outputTypeToJava(outputType),
      keys.toArray[Any].asInstanceOf[Array[K with AnyRef]],
      values:_*
    )).map(outputType.convert)
  }

  override def evalReadOnly(script: Array[Byte], outputType: ScriptOutputType, keys:List[K], values: V*): Future[outputType.R] = {
    delegateRedisClusterCommandAndLift(_.evalReadOnly(
      script,
      LettuceRedisScriptingAsyncCommands.outputTypeToJava(outputType),
      keys.toArray[Any].asInstanceOf[Array[K with AnyRef]],
      values:_*
    )).map(outputType.convert)
  }

  override def scriptExists(digest: String): Future[Boolean] =
    delegateRedisClusterCommandAndLift(_.scriptExists(digest)).map {
      _.asScala.headOption.exists(Boolean2boolean)
    }

  override def scriptExists(digests: List[String]): Future[List[Boolean]] = {
    delegateRedisClusterCommandAndLift(_.scriptExists(digests:_*)).map(_.asScala.map(Boolean2boolean).toList)
  }

  override def scriptFlush: Future[Unit] = {
    delegateRedisClusterCommandAndLift(_.scriptFlush()).map(_ => ())
  }

  override def scriptFlush(mode: RedisScriptingAsyncCommands.FlushMode): Future[Unit] = {
    delegateRedisClusterCommandAndLift(_.scriptFlush(
      mode match {
        case RedisScriptingAsyncCommands.FlushMode.Async => io.lettuce.core.FlushMode.ASYNC
        case RedisScriptingAsyncCommands.FlushMode.Sync => io.lettuce.core.FlushMode.SYNC
      }
    )).map(_ => ())
  }

  override def scriptKill: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.scriptKill()).map(_ => ())

  override def scriptLoad(script: String): Future[String] = {
    delegateRedisClusterCommandAndLift(_.scriptLoad(script))
  }

  override def scriptLoad(script: Array[Byte]): Future[String] =
    delegateRedisClusterCommandAndLift(_.scriptLoad(script))

}

object LettuceRedisScriptingAsyncCommands{
  private[commands] def outputTypeToJava(outputType: ScriptOutputType): io.lettuce.core.ScriptOutputType = outputType match {
    case ScriptOutputType.Boolean => io.lettuce.core.ScriptOutputType.BOOLEAN
    case ScriptOutputType.Integer => io.lettuce.core.ScriptOutputType.INTEGER
    case ScriptOutputType.Multi => io.lettuce.core.ScriptOutputType.MULTI
    case ScriptOutputType.Status => io.lettuce.core.ScriptOutputType.STATUS
    case ScriptOutputType.Value => io.lettuce.core.ScriptOutputType.VALUE
  }
}
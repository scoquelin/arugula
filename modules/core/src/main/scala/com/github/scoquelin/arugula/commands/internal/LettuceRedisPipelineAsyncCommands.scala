package com.github.scoquelin.arugula.commands.internal

import scala.concurrent.Future

import com.github.scoquelin.arugula.api.commands.{RedisAsyncCommands, RedisPipelineAsyncCommands}

private[commands] trait LettuceRedisPipelineAsyncCommands[K, V] extends RedisPipelineAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] { this: RedisAsyncCommands[K, V] =>

  override def pipeline(commands: RedisAsyncCommands[K, V] => List[Future[Any]]): Future[Option[List[Any]]] = {
    //Resorting to basic Futures sequencing after issues trying to implement https://github.com/lettuce-io/lettuce-core/wiki/Pipelining-and-command-flushing#command-flushing
    commands(this).foldLeft(Future.successful(Option.empty[List[Any]]))((commandsOutcome, nextFuture) => commandsOutcome.flatMap {
      case Some(existingList) =>
        nextFuture.map(nextFutureOutcome => Some(existingList ::: List(nextFutureOutcome)))
      case None =>
        nextFuture.map(nextFutureOutcome => Some(List(nextFutureOutcome)))
    })
  }

}


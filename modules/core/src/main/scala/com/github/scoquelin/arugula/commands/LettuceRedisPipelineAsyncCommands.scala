package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

import com.github.scoquelin.arugula.RedisCommandsClient
import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation


private[arugula] trait LettuceRedisPipelineAsyncCommands[K, V] extends RedisPipelineAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] { this: RedisCommandsClient[K, V] =>

  override def pipeline(commands: RedisCommandsClient[K, V] => List[Future[Any]]): Future[Option[List[Any]]] = {
    //Resorting to basic Futures sequencing after issues trying to implement https://github.com/lettuce-io/lettuce-core/wiki/Pipelining-and-command-flushing#command-flushing
    commands(this).foldLeft(Future.successful(Option.empty[List[Any]]))((commandsOutcome, nextFuture) => commandsOutcome.flatMap {
      case Some(existingList) =>
        nextFuture.map(nextFutureOutcome => Some(existingList ::: List(nextFutureOutcome)))
      case None =>
        nextFuture.map(nextFutureOutcome => Some(List(nextFutureOutcome)))
    })
  }

}


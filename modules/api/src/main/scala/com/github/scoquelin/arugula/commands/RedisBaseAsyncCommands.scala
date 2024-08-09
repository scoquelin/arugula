package com.github.scoquelin.arugula.commands

import scala.concurrent.Future

trait RedisBaseAsyncCommands[K, V] {
  def ping: Future[String]
}

object RedisBaseAsyncCommands {
  val InitialCursor: String = "0"

  final case class ScanResults[T](cursor: String, finished: Boolean, values: T)

}
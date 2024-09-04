package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.jdk.CollectionConverters.CollectionHasAsScala

import com.github.scoquelin.arugula.commands.RedisScriptingAsyncCommands.ScriptOutputType

trait RedisScriptingAsyncCommands[K, V] {
  /**
   * Evaluate a script
   * @param script The script to evaluate
   * @param outputType The expected output type
   * @param keys The keys to use in the script
   * @return The result of the script
   */
  def eval(script: String, outputType: ScriptOutputType, keys:K*): Future[outputType.R]

  /**
   * Evaluate a script
   * @param script The script to evaluate
   * @param outputType The expected output type
   * @param keys The keys to use in the script
   * @return The result of the script
   */
  def eval(script: Array[Byte], outputType: ScriptOutputType, keys:K*): Future[outputType.R]

  /**
   * Evaluate a script
   * @param script The script to evaluate
   * @param outputType The expected output type
   * @param keys The keys to use in the script
   * @param values The values to use in the script
   * @return The result of the script
   */
  def eval(script: String, outputType: ScriptOutputType, keys: List[K], values: V*): Future[outputType.R]

  /**
   * Evaluate a script
   * @param script The script to evaluate
   * @param outputType The expected output type
   * @param keys The keys to use in the script
   * @param values The values to use in the script
   * @return The result of the script
   */
  def eval(script: Array[Byte], outputType: ScriptOutputType, keys: List[K], values: V*): Future[outputType.R]

  /**
   * Evaluate a script by its SHA
   * @param sha The SHA of the script to evaluate
   * @param outputType The expected output type
   * @param keys The keys to use in the script
   * @return The result of the script
   */
  def evalSha(sha: String, outputType: ScriptOutputType, keys:K*): Future[outputType.R]

  /**
   * Evaluate a script by its SHA
   * @param sha The SHA of the script to evaluate
   * @param outputType The expected output type
   * @param keys The keys to use in the script
   * @return The result of the script
   */
  def evalSha(sha: String, outputType: ScriptOutputType, keys: List[K], values: V*): Future[outputType.R]

  /**
   * Evaluate a script in read-only mode
   * @param script The script to evaluate
   * @param outputType The expected output type
   * @param keys The keys to use in the script
   * @param values The values to use in the script
   * @return The result of the script
   */
  def evalReadOnly(script: String, outputType: ScriptOutputType, keys:List[K], values: V*): Future[outputType.R]

  /**
   * Evaluate a script in read-only mode
   * @param script The script to evaluate
   * @param outputType The expected output type
   * @param keys The keys to use in the script
   * @param values The values to use in the script
   * @return The result of the script
   */
  def evalReadOnly(script: Array[Byte], outputType: ScriptOutputType, keys:List[K], values: V*): Future[outputType.R]

  /**
   * check if a script exists based on its SHA
   * @param digest The SHA of the script to check
   * @return True if the script exists, false otherwise
   */
  def scriptExists(digest: String): Future[Boolean]

  /**
   * check if a script exists based on its SHA
   * @param digests The SHA of the scripts to check
   * @return A list of booleans indicating if the scripts exist
   */
  def scriptExists(digests: List[String]): Future[List[Boolean]]

  /**
   * Flush all scripts from the script cache
   * @return Unit
   */
  def scriptFlush: Future[Unit]

  /**
   * Flush all scripts from the script cache
   * @param mode The mode to use for flushing
   * @return Unit
   */
  def scriptFlush(mode: RedisScriptingAsyncCommands.FlushMode): Future[Unit]

  /**
   * Kill the currently executing script
   * @return Unit
   */
  def scriptKill: Future[Unit]

  /**
   * Load a script into the script cache
   * @param script The script to load
   * @return The SHA of the script
   */
  def scriptLoad(script: String): Future[String]

  /**
   * Load a script into the script cache
   * @param script The script to load
   * @return The SHA of the script
   */
  def scriptLoad(script: Array[Byte]): Future[String]
}

object RedisScriptingAsyncCommands{

  sealed trait ScriptOutputType {
    type R

    def convert(in: Any): R
  }
  object ScriptOutputType {
    case object Boolean extends ScriptOutputType{
      type R = Boolean

      def convert(in: Any): R = in match {
        case 0L => false
        case 1L => true
        case b: Boolean => b
        case s: String => s.toBoolean
        case _ => throw new IllegalArgumentException(s"Cannot convert $in to Boolean")
      }
    }
    case object Integer extends ScriptOutputType{
      type R = Long

      def convert(in: Any): R = in match {
        case l: Long => l
        case i: Int => i.toLong
        case s: String => s.toLong
        case _ => throw new IllegalArgumentException(s"Cannot convert $in to Int")
      }

    }
    case object Multi extends ScriptOutputType{
      type R = List[Any]

      def convert(in: Any): R = in match {
        case l: List[_] => l
        case l: java.util.List[_] => l.asScala.toList
        case _ => throw new IllegalArgumentException(s"Cannot convert $in to List")
      }
    }
    case object Status extends ScriptOutputType{
      type R = String

      def convert(in: Any): R = in match {
        case s: String => s
        case _ => throw new IllegalArgumentException(s"Cannot convert $in to String")
      }
    }
    case object Value extends ScriptOutputType{
      type R = Any

      def convert(in: Any): R = in
    }
  }

  sealed trait FlushMode

  object FlushMode{
    case object Async extends FlushMode
    case object Sync extends FlushMode
  }
}

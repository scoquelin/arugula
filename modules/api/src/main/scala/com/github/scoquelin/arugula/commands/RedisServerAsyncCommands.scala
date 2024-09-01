package com.github.scoquelin.arugula.commands

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import java.net.InetAddress
import java.nio.charset.{Charset, StandardCharsets}
import java.time.Instant

/**
 * Asynchronous commands for interacting with Redis Server
 *
 * @tparam K The key type
 * @tparam V The value type
 */
trait RedisServerAsyncCommands[K, V] {

  /**
   * Rewrite the AOF in the background
   * @return Unit
   */
  def bgRewriteAof: Future[Unit]

  /**
   * Save the DB in the background
   * @return Unit
   */
  def bgSave: Future[Unit]

  /**
   * Control tracking of keys in the context of server-assisted client cache invalidation.
   * @param enabled True to enable, false to disable
   * @return Unit
   */
  def clientCaching(enabled: Boolean = true): Future[Unit]

  /**
   * Get the current connection name.
   * @return The name of the connection or None if not set
   */
  def clientGetName: Future[Option[K]]

  /**
   * Get the ID of the client we are redirecting the notifications to.
   * @return the ID of the client we are redirecting the notifications to.
   *         The command returns -1 if client tracking is not enabled,
   *         or 0 if client tracking is enabled but we are not redirecting
   *         the notifications to any client.
   * @since 6.0
   */
  def clientGetRedir: Future[Long]


  /**
   * Get the ID of the current connection.
   * @return The ID of the current connection
   */
  def clientId: Future[Long]

  /**
   * Kill the connection of a client
   * @param address The address of the client to kill in the format ip:port
   * @return Unit
   */
  def clientKill(address: String): Future[Unit]

  /**
   * Kill the connection of a client
   * @param args The arguments to kill the connection
   * @return The number of clients killed
   */
  def clientKill(args: RedisServerAsyncCommands.KillArgs): Future[Long]

  /**
   * Get the list of clients connected to the server
   * @return The list of clients
   */
  def clientList: Future[List[RedisServerAsyncCommands.Client]]

  /**
   * Get the list of clients connected to the server
   * @param args The arguments to filter the list of clients
   * @return The list of clients
   */
  def clientList(args: RedisServerAsyncCommands.ClientListArgs): Future[List[RedisServerAsyncCommands.Client]]

  /**
   * Get information and statistics about the client
   * @return The client information
   */
  def clientInfo: Future[RedisServerAsyncCommands.Client]

  /**
   * Sets the client eviction mode for the current connection.
   * @param enabled True to enable, false to disable
   * @return Unit
   */
  def clientNoEvict(enabled: Boolean = true): Future[Unit]

  /**
   * Pause all the clients for the specified amount of time
   * @param timeout The amount of time to pause the clients
   * @return Unit
   */
  def clientPause(timeout: FiniteDuration): Future[Unit]

  /**
   * Set the name of the current connection
   * @param name The name of the connection
   * @return Unit
   */
  def clientSetName(name: K): Future[Unit]

  /**
   * Set a client-specific configuration parameter.
   * Currently the only supported parameters are:
   *  - lib-name - meant to hold the name of the client library that's in use.
   *  - lib-ver - meant to hold the client library's version.
   * @see https://redis.io/commands/client-setinfo
   * @return Unit
   */
  def clientSetInfo(name: String, value: String): Future[Unit]

  /**
   * Instruct the server to track clients using the client tracking feature.
   * @see https://redis.io/commands/client-tracking
   * @param args The arguments to control client tracking
   * @return Unit
   */
  def clientTracking(
    args: RedisServerAsyncCommands.TrackingArgs
  ): Future[Unit]

  /**
   * Unblock a client blocked in a blocking command from a different connection.
   * @param id The ID of the client to unblock
   * @param unblockType The type of unblock to perform
   * @return Unit
   */
  def clientUnblock(id: Long, unblockType: RedisServerAsyncCommands.UnblockType): Future[Long]

  /**
   * Get information about server commands
   * @return A list of commands and their information
   */
  def command: Future[List[RedisServerAsyncCommands.Command]]

  /**
   * Get the total number of commands in this Redis instance
   * @return The number of commands
   */
  def commandCount: Future[Long]

  /**
   * Get information about server commands
   * @param commands The commands to get information for
   * @return A list of commands and their information
   */
  def commandInfo(commands: String*): Future[List[RedisServerAsyncCommands.Command]]

  /**
   * Get the value of a configuration parameter
   * @param parameters The parameters to get the value of
   * @return A map of key-value pairs containing the configuration parameter
   */
  def configGet(parameters: String*): Future[Map[String, String]]

  /**
   * Reset the statistics reported by Redis using the INFO command
   * @return Unit
   */
  def configResetStat: Future[Unit]

  /**
   * Rewrite the configuration file with the in-memory configuration
   * @return Unit
   */
  def configRewrite: Future[Unit]

  /**
   * Set a configuration parameter to the given value
   * @param parameter The parameter to set
   * @param value The value to set the parameter to
   * @return Unit
   */
  def configSet(parameter: String, value: String): Future[Unit]

  /**
   * Set multiple configuration parameters to the given values
   * @param kvs The key-value pairs to set
   * @return Unit
   */
  def configSet(kvs: Map[String, String]): Future[Unit]

  /**
   * Get the number of keys in the current database
   * @return The number of keys
   */
  def dbSize: Future[Long]

  /**
   * Flush all the databases
   * @param mode The flush mode, either async or sync
   * @return Unit
   */
  def flushAll(mode: RedisServerAsyncCommands.FlushMode = RedisServerAsyncCommands.FlushMode.Sync): Future[Unit]

  /**
   * Flush the current database
   * @return Unit
   */
  def flushDb(mode: RedisServerAsyncCommands.FlushMode = RedisServerAsyncCommands.FlushMode.Sync): Future[Unit]


  /**
   * Get information and statistics about the server
   * @return A map of key-value pairs containing the server information
   */
  def info: Future[Map[String, String]]

  /**
   * Get information and statistics about the server in a specific section
   * @param section The section of the server information to get
   * @return A map of key-value pairs containing the server information
   */
  def info(section: String): Future[Map[String, String]]

  /**
   * Get the time of the last successful save to disk
   * @return The time of the last save
   */
  def lastSave: Future[Instant]

  /**
   * Reports the number of bytes that a key and its value require to be stored in RAM.
   * @param key The key to get the memory usage of
   * @return The memory usage of the key
   */
  def memoryUsage(key: K): Future[Option[Long]]

  /**
   * Make the server a replica of another instance.
   * @param host The host of the master instance
   * @param port The port of the master instance
   * @return Unit
   */
  def replicaOf(host: String, port: Int): Future[Unit]

  /**
   * Make the server a replica of no one.
   * This undoes the effect of the replicaOf command.
   * @return Unit
   */
  def replicaOfNoOne: Future[Unit]

  /**
   * Save the DB to disk
   * @return Unit
   */
  def save: Future[Unit]

  /**
   * Make the server a slave of another instance.
   * @param host The host of the master instance
   * @param port The port of the master instance
   * @return Unit
   */
  @deprecated("Use replicaOf instead if server supports it")
  def slaveOf(host: String, port: Int): Future[Unit]

  /**
   * Make the server a slave of no one.
   * This undoes the effect of the slaveOf command.
   * @return Unit
   */
  @deprecated("Use replicaOfNoOne instead if server supports it")
  def slaveOfNoOne: Future[Unit]
}

object RedisServerAsyncCommands {
  sealed trait FlushMode

  object FlushMode {
    case object Async extends FlushMode

    case object Sync extends FlushMode
  }

  case class KillArgs(
    id: Option[Long] = None,
    address: Option[String] = None,
    localAddress: Option[String] = None,
    connectionType: Option[ConnectionType] = None,
    userName: Option[String] = None,
    skipMe: Boolean = false,
  )

  sealed trait ConnectionType
  object ConnectionType {
    case object Normal extends ConnectionType
    case object Master extends ConnectionType
    case object Replica extends ConnectionType
    case object PubSub extends ConnectionType
  }


  case class Client(
    id : Long,
    addr : String,
    fd : Long,
    name : String,
    age : Long,
    idle : Long,
    flags : String,
    db : Long,
    sub : Long,
    psub : Long,
    multi : Long,
    qbuf : Long,
    qbufFree : Long,
    obl : Long,
    oll : Long,
    omem : Long,
    events : String,
    cmd : String
  )

  object Client {

    /**
     * Parse the client information string into a Client object
     * The string will look like:
     * id=74 addr=192.168.65.1:43101 laddr=172.18.0.4:6379 fd=8 name= age=3 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=26 qbuf-free=40928 argv-mem=10 obl=0 oll=0 omem=0 tot-mem=61466 events=r cmd=client user=default redir=-1
     *
     * @param clientString The client information string
     * @return The Client object
     */
    def parseClientInfo(clientString: String): Try[Client] = {
      Try{
        val parts = clientString.split(" ").map(_.split("=")).collect { case Array(k, v) => k -> v }.toMap
        Client(
          id = parts("id").toLong,
          addr = parts("addr"),
          fd = parts("fd").toLong,
          name = parts.getOrElse("name", ""),
          age = parts("age").toLong,
          idle = parts("idle").toLong,
          flags = parts("flags"),
          db = parts("db").toLong,
          sub = parts("sub").toLong,
          psub = parts("psub").toLong,
          multi = parts("multi").toLong,
          qbuf = parts("qbuf").toLong,
          qbufFree = parts("qbuf-free").toLong,
          obl = parts("obl").toLong,
          oll = parts("oll").toLong,
          omem = parts("omem").toLong,
          events = parts("events"),
          cmd = parts("cmd")
        )

      }
    }
  }

  case class ClientListArgs(
    ids: List[Long] = List.empty,
    connectionType: Option[ConnectionType] = None,
  )

  case class TrackingArgs(
    enabled: Boolean = false,
    redirect: Option[Long] = None,
    prefixes: List[String] = List.empty,
    prefixCharset: Charset = StandardCharsets.UTF_8,
    bcast: Boolean = false,
    optIn: Boolean = false,
    optOut: Boolean = false,
    noloop: Boolean = false
  )

  sealed trait UnblockType

  object UnblockType{
    case object Error extends UnblockType
    case object Timeout extends UnblockType
  }

  case class Command(
    name: String,
    arity: Long,
    flags: List[String],
    firstKey: Long,
    lastKey: Long,
    step: Long,
    aclCategories: List[String],
    tips: List[String],

    // Need to implement the following fields:
    //keySpecifications: List[String],
    //subCommands: List[String],
  )
}

// Need to implement the following commands:
//debugCrashAndRecover
//debugHtstats
//debugObject
//debugOom
//debugReload
//debugRestart
//debugSdslen
//debugSegfault
//shutdown
//shutdown
//slowlogGet
//slowlogGet
//slowlogLen
//slowlogReset
//time
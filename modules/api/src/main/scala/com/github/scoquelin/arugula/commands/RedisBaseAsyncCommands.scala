package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait RedisBaseAsyncCommands[K, V] {

  /**
   * Echo the message
   * @param message The message to echo
   * @return The message
   */
  def echo(message: V): Future[V]

  /**
   * Ping the server
   * @return PONG if the server is alive
   */
  def ping: Future[String]

  /**
   * Publish a message to a channel
   * @param channel The channel to publish to
   * @param message The message to publish
   * @return The number of clients that received the message
   */
  def publish(channel: K, message: V): Future[Long]

  /**
   * Get the list of channels
   * @return The list of channels
   */
  def pubsubChannels(): Future[List[K]]

  /**
   * Get the list of channels matching the pattern
   * @param pattern The pattern to match
   * @return The list of channels matching the pattern
   */
  def pubsubChannels(pattern: K): Future[List[K]]

  /**
   * Returns the number of subscribers (not counting clients subscribed to patterns) for the specified channels.
   * @param channels The channels to get the number of subscribers for
   * @return The number of subscribers for each channel
   */
  def pubsubNumsub(channels: K*): Future[Map[K, Long]]

  /**
   * Returns the number of pattern subscribers
   * @return The number of pattern subscribers
   */
  def pubsubNumpat(): Future[Long]

  /**
   * Instructs Redis to disconnect the connection.
   * Note that if auto-reconnect is enabled then it will auto-reconnect if the connection was disconnected.
   * @return Unit
   */
  def quit(): Future[Unit]

  /**
   * Switch the connection to read-only mode
   * @return Unit
   */
  def readOnly(): Future[Unit]

  /**
   * Switch the connection to read-write mode
   * @return Unit
   */
  def readWrite(): Future[Unit]

  /**
   * Get the role of the server.
   * The role can be one of:
   * - Master
   * - Slave
   * - Sentinel
   * Each role has different information associated with it.
   * Match on the role to get the specific information.
   * @see https://redis.io/commands/role
   * @return The role of the server
   */
  def role(): Future[RedisBaseAsyncCommands.Role]

  /**
   * Wait for replication
   * @param replicas The number of replicas to wait for
   * @param timeout The timeout to wait for replication
   * @return The number of replicas that acknowledged the write
   */
  def waitForReplication(
    replicas: Int,
    timeout: FiniteDuration
  ): Future[Long]

}

object RedisBaseAsyncCommands {
  val InitialCursor: String = "0"

  final case class ScanResults[T](cursor: String, finished: Boolean, values: T)

  sealed trait LinkStatus
  object LinkStatus {
    case object Connect extends LinkStatus
    case object Connecting extends LinkStatus
    case object Sync extends LinkStatus
    case object Connected extends LinkStatus

    def apply(status: String): LinkStatus = status match {
      case "connect" => Connect
      case "connecting" => Connecting
      case "sync" => Sync
      case "connected" => Connected
      case _ => throw new IllegalArgumentException(s"Unknown link status: $status")
    }
  }

  /**
   * The role of the server
   */
  sealed trait Role


  object Role {
    /**
     * The master role
     * @param replicationOffset The replication offset
     * @param replicas The list of replicas
     */
    case class Master(
     replicationOffset: Long,
      replicas: List[Replica]
    ) extends Role

    /**
     * The slave role
     * @param masterIp The IP of the master
     * @param masterPort The port of the master
     * @param masterReplicationOffset The replication offset of the master
     * @param linkStatus The link status
     * @param replicationOffset The replication offset
     */
    case class Slave(
      masterIp: String,
      masterPort: Int,
      masterReplicationOffset: Long,
      linkStatus: LinkStatus,
      replicationOffset: Long,
    ) extends Role

    /**
     * The sentinel role
     * @param masterNames The list of master names
     */
    case class Sentinel(
      masterNames: List[String]
    ) extends Role
  }

  /**
   * A replica
   * @param host The host of the replica
   * @param port The port of the replica
   * @param replicationOffset The replication offset of the replica
   */
  case class Replica(
    host: String,
    port: Int,
    replicationOffset: Long
  )

}


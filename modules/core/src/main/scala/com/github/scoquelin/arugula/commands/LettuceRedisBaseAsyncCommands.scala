package com.github.scoquelin.arugula.commands

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation

private[arugula] trait LettuceRedisBaseAsyncCommands[K, V] extends RedisBaseAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {

  override def echo(message: V): Future[V] =
    delegateRedisClusterCommandAndLift(_.echo(message))

  override def ping: Future[String] =
    delegateRedisClusterCommandAndLift(_.ping())

  override def publish(channel: K, message: V): Future[Long] =
    delegateRedisClusterCommandAndLift(_.publish(channel, message)).map(Long2long)

  override def pubsubChannels(): Future[List[K]] =
    delegateRedisClusterCommandAndLift(_.pubsubChannels()).map(_.toList)

  override def pubsubChannels(pattern: K): Future[List[K]] =
    delegateRedisClusterCommandAndLift(_.pubsubChannels(pattern)).map(_.toList)

  override def pubsubNumsub(channels: K*): Future[Map[K, Long]] =
    delegateRedisClusterCommandAndLift(_.pubsubNumsub(channels: _*)).map(_.asScala.map{
      case (k, v) => (k, Long2long(v))
    }.toMap)

  override def pubsubNumpat(): Future[Long] =
    delegateRedisClusterCommandAndLift(_.pubsubNumpat()).map(Long2long)

  override def quit(): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.quit()).map(_ => ())

  override def readOnly(): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.readOnly()).map(_ => ())

  override def readWrite(): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.readWrite()).map(_ => ())

  override def role(): Future[RedisBaseAsyncCommands.Role] = {
    delegateRedisClusterCommandAndLift(_.role()).flatMap { info =>
      val role = info.get(0).asInstanceOf[String]
      role match {
        case "master" =>
          Future.successful(RedisBaseAsyncCommands.Role.Master(
            replicationOffset = info.get(1).asInstanceOf[java.lang.Long],
            replicas = info.get(2).asInstanceOf[java.util.List[AnyRef]].asScala.map { replicaInfo =>
              val parts = replicaInfo.asInstanceOf[java.util.List[AnyRef]]
              RedisBaseAsyncCommands.Replica(
               host = parts.get(0).asInstanceOf[String],
                port = parts.get(1).asInstanceOf[String].toIntOption.getOrElse(0),
                replicationOffset = parts.get(2).asInstanceOf[String].toLongOption.getOrElse(0),
              )
            }.toList
          ))

        case "slave" =>
          Future.successful(RedisBaseAsyncCommands.Role.Slave(
            masterIp = info.get(1).asInstanceOf[String],
            masterPort = info.get(2).asInstanceOf[java.lang.Integer],
            masterReplicationOffset = info.get(3).asInstanceOf[String].toLongOption.getOrElse(0),
            linkStatus = RedisBaseAsyncCommands.LinkStatus(info.get(4).asInstanceOf[String]),
            replicationOffset = info.get(5).asInstanceOf[java.lang.Integer].longValue(),
          ))

        case "sentinel" =>
          Future.successful(RedisBaseAsyncCommands.Role.Sentinel(
            masterNames = info.get(1).asInstanceOf[java.util.List[String]].asScala.toList
          ))

        case _ =>
          Future.failed(new IllegalStateException("Role command did not return expected values"))
      }
    }
  }


  override def waitForReplication(
    replicas: Int,
    timeout: FiniteDuration
  ): Future[Long] =
    delegateRedisClusterCommandAndLift(_.waitForReplication(replicas, timeout.toMillis)).map(Long2long)

}

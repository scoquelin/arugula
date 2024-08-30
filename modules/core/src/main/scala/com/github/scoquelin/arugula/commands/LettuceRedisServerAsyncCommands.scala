package com.github.scoquelin.arugula.commands

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava, MapHasAsScala}
import scala.util.Try

import com.github.scoquelin.arugula.internal.LettuceRedisCommandDelegation
import io.lettuce.core.RedisFuture

import java.time.Instant
import java.{lang, util}


private[arugula] trait LettuceRedisServerAsyncCommands[K, V] extends RedisServerAsyncCommands[K, V] with LettuceRedisCommandDelegation[K, V] {

  override def bgRewriteAof: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.bgrewriteaof()).map(_ => ())

  override def bgSave: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.bgsave()).map(_ => ())

  override def clientCaching(enabled: Boolean = true): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.clientCaching(enabled)).map(_ => ())

  override def clientGetName: Future[Option[K]] =
    delegateRedisClusterCommandAndLift(_.clientGetname()).map(Option(_))

  override def clientGetRedir: Future[Long] =
    delegateRedisClusterCommandAndLift(_.clientGetredir()).map(Long2long)

  override def clientId: Future[Long] =
    delegateRedisClusterCommandAndLift(_.clientId()).map(Long2long)

  override def clientKill(address: String): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.clientKill(address)).map(_ => ())

  override def clientKill(args: RedisServerAsyncCommands.KillArgs): Future[Long] = {
    val killArgs = args.connectionType match {
      case Some(RedisServerAsyncCommands.ConnectionType.Normal) => io.lettuce.core.KillArgs.Builder.typeNormal()
      case Some(RedisServerAsyncCommands.ConnectionType.Master) => io.lettuce.core.KillArgs.Builder.typeMaster()
      case Some(RedisServerAsyncCommands.ConnectionType.Replica) => io.lettuce.core.KillArgs.Builder.typeSlave()
      case Some(RedisServerAsyncCommands.ConnectionType.PubSub) => io.lettuce.core.KillArgs.Builder.typePubsub()
      case _ => new io.lettuce.core.KillArgs()
    }
    if(args.skipMe) killArgs.skipme()
    args.id.foreach(killArgs.id)
    args.address.foreach(killArgs.addr)
    args.localAddress.foreach(killArgs.laddr)
    args.userName.foreach(killArgs.user)
    delegateRedisClusterCommandAndLift(_.clientKill(killArgs)).map(Long2long)
  }

  override def clientList: Future[List[RedisServerAsyncCommands.Client]] =
    delegateRedisClusterCommandAndLift(_.clientList()).map{ clientListStr =>
      clientListStr.split("\\r?\\n").toList
        .flatMap( RedisServerAsyncCommands.Client.parseClientInfo(_).toOption )
    }

  override def clientList(args: RedisServerAsyncCommands.ClientListArgs): Future[List[RedisServerAsyncCommands.Client]] = {
    val clientListArgs = args.connectionType match {
      case Some(RedisServerAsyncCommands.ConnectionType.Normal) => io.lettuce.core.ClientListArgs.Builder.typeNormal()
      case Some(RedisServerAsyncCommands.ConnectionType.Master) => io.lettuce.core.ClientListArgs.Builder.typeMaster()
      case Some(RedisServerAsyncCommands.ConnectionType.Replica) => io.lettuce.core.ClientListArgs.Builder.typeReplica()
      case Some(RedisServerAsyncCommands.ConnectionType.PubSub) => io.lettuce.core.ClientListArgs.Builder.typePubsub()
      case _ => new io.lettuce.core.ClientListArgs()
    }
    if(args.ids.nonEmpty) clientListArgs.ids(args.ids: _*)
    delegateRedisClusterCommandAndLift(_.clientList(clientListArgs)).map{ clientListStr =>
      clientListStr.split("\\r?\\n").toList
        .flatMap( RedisServerAsyncCommands.Client.parseClientInfo(_).toOption )
    }
  }

  override def clientInfo: Future[RedisServerAsyncCommands.Client] =
    delegateRedisClusterCommandAndLift(_.clientInfo()).flatMap { clientInfoStr =>
      RedisServerAsyncCommands.Client.parseClientInfo(clientInfoStr).toOption match {
        case Some(client) => Future.successful(client)
        case None => Future.failed(new IllegalArgumentException(s"Unable to parse client info: $clientInfoStr"))
      }
    }

  override def clientNoEvict(enabled: Boolean = true): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.clientNoEvict(enabled)).map(_ => ())

  override def clientPause(timeout: FiniteDuration): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.clientPause(timeout.toMillis)).map(_ => ())

  override def clientSetName(name: K): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.clientSetname(name)).map(_ => ())

  override def clientSetInfo(name: String, value: String): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.clientSetinfo(name, value)).map(_ => ())

  override def clientTracking(
    args: RedisServerAsyncCommands.TrackingArgs
  ): Future[Unit] ={
    val trackingArgs = io.lettuce.core.TrackingArgs.Builder.enabled(args.enabled)
    args.redirect.foreach(trackingArgs.redirect)
    if(args.prefixes.nonEmpty) trackingArgs.prefixes(args.prefixCharset, args.prefixes: _*)
    if(args.bcast) trackingArgs.bcast()
    if(args.optIn) trackingArgs.optin()
    if(args.optOut) trackingArgs.optout()
    if(args.noloop) trackingArgs.noloop()
    delegateRedisClusterCommandAndLift(_.clientTracking(trackingArgs)).map(_ => ())
  }

  override def clientUnblock(id: Long, unblockType: RedisServerAsyncCommands.UnblockType): Future[Long] = {
    delegateRedisClusterCommandAndLift(_.clientUnblock(
      id,
      unblockType match {
        case RedisServerAsyncCommands.UnblockType.Error => io.lettuce.core.UnblockType.ERROR
        case RedisServerAsyncCommands.UnblockType.Timeout => io.lettuce.core.UnblockType.TIMEOUT
      }
    )).map(Long2long)
  }

  override def command: Future[List[RedisServerAsyncCommands.Command]] =
    delegateRedisClusterCommandAndLift(_.command()).map(_.asScala.map{ cmd =>
      LettuceRedisServerAsyncCommands.parseCommand(cmd)

    }.toList)

  override def commandCount: Future[Long] =
    delegateRedisClusterCommandAndLift(_.commandCount()).map(Long2long)

  override def commandInfo(commands: String*): Future[List[RedisServerAsyncCommands.Command]] =
    delegateRedisClusterCommandAndLift(_.commandInfo(commands: _*)).map(_.asScala.map{ cmd =>
      LettuceRedisServerAsyncCommands.parseCommand(cmd)
    }.toList)

  override def configGet(parameters: String*): Future[Map[String, String]] =
    delegateRedisClusterCommandAndLift(_.configGet(parameters:_*)).map(_.asScala.toMap)

  override def configResetStat: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.configResetstat()).map(_ => ())

  override def configRewrite: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.configRewrite()).map(_ => ())

  override def configSet(parameter: String, value: String): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.configSet(parameter, value)).map(_ => ())

  override def configSet(kvs: Map[String, String]): Future[Unit] = {
    val javaMap: java.util.Map[String, String] = new java.util.HashMap
    kvs.foreach { case (k, v) => javaMap.put(k, v) }
    delegateRedisClusterCommandAndLift(_.configSet(javaMap)).map(_ => ())
  }

  override def dbSize: Future[Long] =
    delegateRedisClusterCommandAndLift(_.dbsize()).map(Long2long)

  override def flushAll(mode: RedisServerAsyncCommands.FlushMode = RedisServerAsyncCommands.FlushMode.Sync): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.flushall(mode match {
      case RedisServerAsyncCommands.FlushMode.Async => io.lettuce.core.FlushMode.ASYNC
      case RedisServerAsyncCommands.FlushMode.Sync => io.lettuce.core.FlushMode.SYNC
    })).map(_ => ())


  override def flushDb(mode: RedisServerAsyncCommands.FlushMode = RedisServerAsyncCommands.FlushMode.Sync): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.flushdb(mode match {
      case RedisServerAsyncCommands.FlushMode.Async => io.lettuce.core.FlushMode.ASYNC
      case RedisServerAsyncCommands.FlushMode.Sync => io.lettuce.core.FlushMode.SYNC
    })).map(_ => ())

  override def info: Future[Map[String, String]] =
    delegateRedisClusterCommandAndLift(_.info()).map(LettuceRedisServerAsyncCommands.parseInfo)

  override def info(section: String): Future[Map[String, String]] =
    delegateRedisClusterCommandAndLift(_.info(section)).map(LettuceRedisServerAsyncCommands.parseInfo)

  override def lastSave: Future[Instant] =
    delegateRedisClusterCommandAndLift(_.lastsave()).map(_.toInstant)

  override def memoryUsage(key: K): Future[Option[Long]] =
    delegateRedisClusterCommandAndLift(_.memoryUsage(key)).map(Option(_).map(Long2long))

  override def replicaOf(host: String, port: Int): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.replicaof(host, port)).map(_ => ())

  override def replicaOfNoOne: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.replicaofNoOne()).map(_ => ())

  override def save: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.save()).map(_ => ())

  override def slaveOf(host: String, port: Int): Future[Unit] =
    delegateRedisClusterCommandAndLift(_.slaveof(host, port)).map(_ => ())

  override def slaveOfNoOne: Future[Unit] =
    delegateRedisClusterCommandAndLift(_.slaveofNoOne()).map(_ => ())

}

object LettuceRedisServerAsyncCommands {
  def parseInfo(info: String): Map[String, String] =
    info
      .split("\\r?\\n")
      .toList
      .map(_.split(":", 2).toList)
      .collect { case k :: v :: Nil => (k, v) }
      .toMap

  def parseCommand(cmd: AnyRef): RedisServerAsyncCommands.Command = {
    val cmdInfo = cmd.asInstanceOf[java.util.List[AnyRef]]
    RedisServerAsyncCommands.Command(
      name = cmdInfo.get(0).asInstanceOf[String],
      arity = cmdInfo.get(1).asInstanceOf[lang.Long],
      flags = cmdInfo.get(2).asInstanceOf[util.List[String]].asScala.toList,
      firstKey = cmdInfo.get(3).asInstanceOf[lang.Long],
      lastKey = cmdInfo.get(4).asInstanceOf[lang.Long],
      step = cmdInfo.get(5).asInstanceOf[lang.Long],
      aclCategories = Try(cmdInfo.get(6).asInstanceOf[util.List[String]].asScala.toList).getOrElse(List.empty),
      tips = Try(cmdInfo.get(7).asInstanceOf[util.List[String]].asScala.toList).getOrElse(List.empty)
    )
  }
}
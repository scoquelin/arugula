package com.github.scoquelin.arugula.config

import com.typesafe.config.Config
import io.lettuce.core.resource.{ClientResources => JClientResources}

case class LettuceRedisClientConfig(host: String, port: Int, clientResources: JClientResources = io.lettuce.core.resource.DefaultClientResources.create())

object LettuceRedisClientConfig {
  def fromConfig(config: Config): LettuceRedisClientConfig = {
    LettuceRedisClientConfig(
      host = config.getString("host"),
      port = config.getInt("port")
    )
  }
}

package com.github.scoquelin.arugula.config

import io.lettuce.core.resource.{ClientResources => JClientResources}

case class LettuceRedisClientConfig(host: String, port: Int, clientResources: JClientResources = io.lettuce.core.resource.DefaultClientResources.create())



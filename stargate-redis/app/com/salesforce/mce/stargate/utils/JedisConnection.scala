/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.mce.stargate.utils

import java.net.URI
import javax.net.ssl.SSLSocketFactory

import scala.collection.JavaConverters

import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import play.api.Configuration
import redis.clients.jedis._

object JedisConnection {
  val defaultTimeoutMilis = 2000
  val defaultMaxAttempts = 5
  val config = Configuration(ConfigFactory.load())
  val clusterNodeUrls = config.get[Seq[String]]("com.salesforce.mce.stargate.redis.clusterNodeUrls")

  val cluster: JedisCluster = {
    val hostsAndPortsAndPasswords: Seq[(HostAndPort, Option[String], Boolean)] = clusterNodeUrls.map { url =>
      val uri = new URI(url)
      val password = Option(uri.getUserInfo).map(_.split(":")(1))
      val ssl = uri.getScheme == "rediss"
      (new HostAndPort(uri.getHost, uri.getPort), password, ssl)
    }
    val clusterNodes = JavaConverters.setAsJavaSet(hostsAndPortsAndPasswords.map(_._1).toSet)
    val useSsl = hostsAndPortsAndPasswords.headOption.exists(_._3)

    val clientConfig = DefaultJedisClientConfig.builder()
      .connectionTimeoutMillis(defaultTimeoutMilis)
      .socketTimeoutMillis(defaultTimeoutMilis)
      .ssl(useSsl)

    hostsAndPortsAndPasswords(0)._2.foreach { password =>
      clientConfig.password(password)
    }

    new JedisCluster(
      clusterNodes,
      clientConfig.build(),
      defaultMaxAttempts,
      new GenericObjectPoolConfig[Connection]()
    )
  }

  def flushDB(): Unit = {
    clusterNodeUrls.foreach { url =>
      val uri = new URI(url)
      val ssl = uri.getScheme == "rediss"
      val jedis = if (ssl) {
        new Jedis(uri, DefaultJedisClientConfig.builder().ssl(true).build())
      } else {
        new Jedis(uri)
      }
      try {
        jedis.flushDB()
      } finally {
        jedis.close()
      }
    }
  }
}

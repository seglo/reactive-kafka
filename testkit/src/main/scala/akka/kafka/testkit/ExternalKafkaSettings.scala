/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit

import akka.annotation.InternalApi
import akka.kafka.internal.ConfigSettings
import com.typesafe.config.Config
import org.apache.kafka.clients.admin.AdminClientConfig

object ExternalKafkaSettings {
  def apply(config: Config): ExternalKafkaSettings = {
    val enabled = config.getBoolean("enabled")
    val adminClientProperties =
      ConfigSettings.parseKafkaClientsProperties(config.getConfig("admin-client.kafka-clients"))
    new ExternalKafkaSettings(adminClientProperties, enabled)
  }
}

class ExternalKafkaSettings @InternalApi private[kafka] (
    val adminClientProperties: Map[String, String],
    val enabled: Boolean
) {
  def bootstrapServers: String = {
    adminClientProperties.getOrElse(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
      throw new IllegalStateException("A 'bootstrap.servers' for the AdminClient when using external Kafka")
    )
  }
}

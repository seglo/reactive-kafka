/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import akka.kafka.testkit.internal.KafkaTestKit
import akka.kafka.{ConsumerSettings, ProducerSettings}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.scalatest.BeforeAndAfterEach

import scala.jdk.CollectionConverters._

object ExternalKafkaLike {
  val topicsCreated = new ConcurrentLinkedQueue[String]()
}

trait ExternalKafkaLike extends KafkaSpec with KafkaTestKit with BeforeAndAfterEach {
  this: ScalatestKafkaSpec =>

  import ExternalKafkaLike._

  val externalSettings = settings.externalKafkaSettings

  // FIXME
//  override def bootstrapServers: String =
//    if (externalSettings.enabled)
//      externalSettings.bootstrapServers
//    else
//      super.bootstrapServers

  override def createTopic(suffix: Int,
                           partitions: Int,
                           replication: Int,
                           config: java.util.Map[String, String]): String =
    enabledOrElse(
      {
        val topic = super.createTopic(suffix, partitions, replication, config)
        topicsCreated.add(topic)
        topic
      },
      super.createTopic(suffix, partitions, replication, config)
    )

  override def setUp(): Unit = enabledOrElse(
    setUpClients(),
    super.setUp()
  )

  override def cleanUp(): Unit = enabledOrElse(
    cleanUpClients(),
    super.cleanUp()
  )

  override def afterEach(): Unit = enabled {
    super.afterEach()
    adminClient.deleteTopics(topicsCreated)
    topicsCreated.clear()
  }

  override def adminClientDefaults: util.Map[String, AnyRef] =
    enabledOrElse({
      val config = new java.util.HashMap[String, AnyRef]()
      config.putAll(externalSettings.adminClientProperties.asJava)
      config
    }, super.adminClientDefaults)

  override def producerDefaults[K, V](keySerializer: Serializer[K],
                                      valueSerializer: Serializer[V]): ProducerSettings[K, V] = enabledOrElse(
    ProducerSettings(system, keySerializer, valueSerializer)
      .withBootstrapServers(externalSettings.bootstrapServers),
    super.producerDefaults(keySerializer, valueSerializer)
  )

  //.withProperty(ProducerConfig.RETRIES_CONFIG, "0")
  //.withProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
  //.withProperty(ProducerConfig.ACKS_CONFIG, "all")
  //.withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

  override def producerDefaults: ProducerSettings[String, String] = enabledOrElse(
    producerDefaults(StringSerializer, StringSerializer),
    super.producerDefaults
  )

  override def consumerDefaults: ConsumerSettings[String, String] = enabledOrElse(
    consumerDefaults(StringDeserializer, StringDeserializer),
    super.consumerDefaults
  )

  override def consumerDefaults[K, V](keyDeserializer: Deserializer[K],
                                      valueDeserializer: Deserializer[V]): ConsumerSettings[K, V] = enabledOrElse(
    ConsumerSettings(system, keyDeserializer, valueDeserializer)
      .withBootstrapServers(externalSettings.bootstrapServers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
    super.consumerDefaults(keyDeserializer, valueDeserializer)
  )

  private def enabled(_enabled: => Unit): Unit =
    if (externalSettings.enabled) _enabled

  private def enabledOrElse[T](enabled: => T, default: => T): T =
    if (externalSettings.enabled) enabled
    else default
}

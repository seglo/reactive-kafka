/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.kafka.benchmarks.app.RunTestCommand
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.jdk.CollectionConverters._

case class KafkaConsumerTestFixture(topic: String, msgCount: Int, consumer: KafkaConsumer[Array[Byte], String]) {
  def close(): Unit = consumer.close()
}

object KafkaConsumerFixtures extends PerfFixtureHelpers {

  def noopFixtureGen(c: RunTestCommand) = FixtureGen[KafkaConsumerTestFixture](
    c,
    msgCount => {
      KafkaConsumerTestFixture("topic", msgCount, null)
    }
  )

  def filledTopics(c: RunTestCommand) = FixtureGen[KafkaConsumerTestFixture](
    c,
    msgCount => {
      fillTopic(c.filledTopic, c.kafkaTestKit)
      val consumerJavaProps = createConsumerSettings(c.kafkaTestKit).getProperties
      val consumer =
        new KafkaConsumer[Array[Byte], String](consumerJavaProps, new ByteArrayDeserializer, new StringDeserializer)
      consumer.subscribe(Set(c.filledTopic.topic).asJava)
      KafkaConsumerTestFixture(c.filledTopic.topic, msgCount, consumer)
    }
  )
}

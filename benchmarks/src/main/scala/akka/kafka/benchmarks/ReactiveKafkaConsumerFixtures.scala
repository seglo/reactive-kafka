/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.Subscriptions
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord

case class ReactiveKafkaConsumerTestFixture[T](topic: String,
                                               msgCount: Int,
                                               source: Source[T, Control],
                                               numberOfPartitions: Int)

object ReactiveKafkaConsumerFixtures extends PerfFixtureHelpers {

  def plainSources(c: RunTestCommand)(implicit actorSystem: ActorSystem) =
    FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](
      c,
      msgCount => {
        fillTopic(c.filledTopic, c.kafkaTestKit)
        val settings = createConsumerSettings(c.kafkaTestKit)
        val source = Consumer.plainSource(settings, Subscriptions.topics(c.filledTopic.topic))
        ReactiveKafkaConsumerTestFixture(c.filledTopic.topic, msgCount, source, c.numberOfPartitions)
      }
    )

  def committableSources(c: RunTestCommand)(implicit actorSystem: ActorSystem) =
    FixtureGen[ReactiveKafkaConsumerTestFixture[CommittableMessage[Array[Byte], String]]](
      c,
      msgCount => {
        fillTopic(c.filledTopic, c.kafkaTestKit)
        val settings = createConsumerSettings(c.kafkaTestKit)
        val source = Consumer.committableSource(settings, Subscriptions.topics(c.filledTopic.topic))
        ReactiveKafkaConsumerTestFixture(c.filledTopic.topic, msgCount, source, c.numberOfPartitions)
      }
    )

  def noopFixtureGen(c: RunTestCommand) =
    FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](
      c,
      msgCount => {
        ReactiveKafkaConsumerTestFixture("topic", msgCount, null, c.numberOfPartitions)
      }
    )

}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.TransactionalMessage
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.Transactional
import akka.kafka.{ConsumerMessage, Subscriptions}
import akka.stream.scaladsl.{Flow, Source}

import scala.concurrent.duration.FiniteDuration

case class ReactiveKafkaTransactionTestFixture[SOut, FIn, FOut](sourceTopic: String,
                                                                sinkTopic: String,
                                                                msgCount: Int,
                                                                source: Source[SOut, Control],
                                                                flow: Flow[FIn, FOut, NotUsed])

object ReactiveKafkaTransactionFixtures extends PerfFixtureHelpers {
  type Key = Array[Byte]
  type Val = String
  type PassThrough = ConsumerMessage.PartitionOffset
  type KTransactionMessage = TransactionalMessage[Key, Val]
  type KProducerMessage = Envelope[Key, Val, PassThrough]
  type KResult = Results[Key, Val, PassThrough]

  def transactionalSourceAndSink(c: RunTestCommand, commitInterval: FiniteDuration)(implicit actorSystem: ActorSystem) =
    FixtureGen[ReactiveKafkaTransactionTestFixture[KTransactionMessage, KProducerMessage, KResult]](
      c,
      msgCount => {
        fillTopic(c.filledTopic, c.kafkaTestKit)
        val sinkTopic = randomId()

        val consumerSettings = createConsumerSettings(c.kafkaTestKit)
        val source: Source[KTransactionMessage, Control] =
          Transactional.source(consumerSettings, Subscriptions.topics(c.filledTopic.topic))

        val producerSettings = createProducerSettings(c.kafkaTestKit).withEosCommitInterval(commitInterval)
        val flow: Flow[KProducerMessage, KResult, NotUsed] = Transactional.flow(producerSettings, randomId())

        ReactiveKafkaTransactionTestFixture[KTransactionMessage, KProducerMessage, KResult](c.filledTopic.topic,
                                                                                            sinkTopic,
                                                                                            msgCount,
                                                                                            source,
                                                                                            flow)
      }
    )

  def noopFixtureGen(c: RunTestCommand) =
    FixtureGen[ReactiveKafkaTransactionTestFixture[KTransactionMessage, KProducerMessage, KResult]](c, msgCount => {
      ReactiveKafkaTransactionTestFixture("sourceTopic", "sinkTopic", msgCount, source = null, flow = null)
    })
}

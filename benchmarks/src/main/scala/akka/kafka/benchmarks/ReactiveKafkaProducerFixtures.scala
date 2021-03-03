/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord

object ReactiveKafkaProducerFixtures extends PerfFixtureHelpers {

  val Parallelism = 10000

  type K = Array[Byte]
  type V = String
  type In[PassThrough] = Envelope[K, V, PassThrough]
  type Out[PassThrough] = Results[K, V, PassThrough]
  type FlowType[PassThrough] = Flow[In[PassThrough], Out[PassThrough], NotUsed]

  case class ReactiveKafkaProducerTestFixture[PassThrough](topic: String,
                                                           msgCount: Int,
                                                           msgSize: Int,
                                                           flow: FlowType[PassThrough],
                                                           numberOfPartitions: Int)

  def flowFixture(c: RunTestCommand)(implicit actorSystem: ActorSystem) =
    FixtureGen[ReactiveKafkaProducerTestFixture[Int]](
      c,
      msgCount => {
        val flow: FlowType[Int] = Producer.flexiFlow(createProducerSettings(c.kafkaTestKit))
        fillTopic(c.filledTopic.copy(msgCount = 1), c.kafkaTestKit)
        ReactiveKafkaProducerTestFixture(c.filledTopic.topic, msgCount, c.msgSize, flow, c.numberOfPartitions)
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

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorSystem
import akka.kafka.tests.scaladsl.LogCapturing
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}

class MisconfiguredConsumerSpec
    extends TestKit(ActorSystem())
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Eventually
    with IntegrationPatience
    with LogCapturing {

  implicit val materializer: Materializer = ActorMaterializer()

  def bootstrapServers = "nowhere:6666"

  "Failing consumer construction" must {
    "be signalled to the stream by single sources" in assertAllStagesStopped {
      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group")
      val result = Consumer
        .plainSource(consumerSettings, Subscriptions.topics("topic"))
        .runWith(Sink.head)

      result.failed.futureValue shouldBe a[org.apache.kafka.common.KafkaException]
    }

    "be signalled to the stream by single sources with external offset" in assertAllStagesStopped {
      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group")
      val result = Consumer
        .plainSource(consumerSettings, Subscriptions.assignmentWithOffset(new TopicPartition("topic", 0) -> 3123L))
        .runWith(Sink.ignore)

      result.failed.futureValue shouldBe a[org.apache.kafka.common.KafkaException]
    }

    "be signalled to the stream by partitioned sources" in assertAllStagesStopped {
      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group")
      val result = Consumer
        .plainPartitionedSource(consumerSettings, Subscriptions.topics("topic"))
        .runWith(Sink.head)

      result.failed.futureValue shouldBe a[org.apache.kafka.common.KafkaException]
    }
  }
}

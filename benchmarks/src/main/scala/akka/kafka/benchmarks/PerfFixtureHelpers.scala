/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.time.Duration
import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.kafka.ProducerSettings
import akka.kafka.testkit.internal.KafkaTestKit
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.postfixOps

object PerfFixtureHelpers {
  def stringOfSize(size: Int) = new String(Array.fill(size)('0'))

  def randomId(): String = UUID.randomUUID().toString

  case class FilledTopic(
      msgCount: Int,
      msgSize: Int,
      numberOfPartitions: Int = 1,
      replicationFactor: Int = 1,
      topic: String = randomId()
  ) {
    def freshTopic: FilledTopic = copy(topic = randomId())
  }
}

private[benchmarks] trait PerfFixtureHelpers extends LazyLogging {
  import PerfFixtureHelpers._

  val producerTimeout = 6 minutes
  val logPercentStep = 25
  val adminClientCloseTimeout = Duration.ofSeconds(5)
  val producerCloseTimeout = adminClientCloseTimeout

  def randomId(): String = PerfFixtureHelpers.randomId()

  def createConsumerSettings(kafkaTestKit: KafkaTestKit) =
    kafkaTestKit
      .consumerDefaults(new ByteArrayDeserializer, new StringDeserializer)
      .withGroupId(randomId())
      .withClientId(randomId())
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def createProducerSettings(kafkaTestKit: KafkaTestKit): ProducerSettings[Array[Byte], String] =
    kafkaTestKit.producerDefaults(new ByteArraySerializer, new StringSerializer)

  def fillTopic(ft: FilledTopic, kafkaTestKit: KafkaTestKit): Unit =
    initTopicAndProducer(ft, kafkaTestKit)

  def createTopic(ft: FilledTopic, kafkaTestKit: KafkaTestKit): Producer[Array[Byte], String] = {
    //val admin = kafkaTestKit.adminClient
    val producer = createTopicAndFill(ft, kafkaTestKit)
    //FIXME close this in the test class?
    //admin.close(adminClientCloseTimeout)
    producer
  }

  private def initTopicAndProducer(ft: FilledTopic, kafkaTestKit: KafkaTestKit): Unit = {
    val admin = kafkaTestKit.adminClient
    val existing = admin.listTopics().names().get(10, TimeUnit.SECONDS)
    if (existing.contains(ft.topic)) {
      logger.info(s"Reusing existing topic $ft")
    } else {
      val producer = createTopicAndFill(ft, kafkaTestKit)
      producer.close(producerCloseTimeout)
    }
    //FIXME close this in the test class?
    //admin.close(adminClientCloseTimeout)
  }

  private def createTopicAndFill(ft: FilledTopic, kafkaTestKit: KafkaTestKit): Producer[Array[Byte], String] = {
    kafkaTestKit.createTopicWithName(ft.topic,
                                     ft.numberOfPartitions,
                                     ft.replicationFactor,
                                     new util.HashMap[String, String]())
    // fill topic with messages
    val producer = kafkaTestKit.producerDefaults(new ByteArraySerializer, new StringSerializer).createKafkaProducer()
    val lastElementStoredPromise = Promise[Unit]()
    val loggedStep = if (ft.msgCount > logPercentStep) ft.msgCount / (100 / logPercentStep) else 1
    val msg = stringOfSize(ft.msgSize)
    for (i <- 0L to ft.msgCount.toLong) {
      if (!lastElementStoredPromise.isCompleted) {
        val partition: Int = (i % ft.numberOfPartitions).toInt
        producer.send(
          new ProducerRecord[Array[Byte], String](ft.topic, partition, null, msg),
          new Callback {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit =
              if (e == null) {
                if (i % loggedStep == 0)
                  logger.info(s"Written $i elements to Kafka (${100 * i / ft.msgCount}%)")
                if (i >= ft.msgCount - 1 && !lastElementStoredPromise.isCompleted)
                  lastElementStoredPromise.success(())
              } else {
                if (!lastElementStoredPromise.isCompleted) {
                  e.printStackTrace()
                  lastElementStoredPromise.failure(e)
                }
              }
          }
        )
      }
    }
    val lastElementStoredFuture = lastElementStoredPromise.future
    Await.result(lastElementStoredFuture, atMost = producerTimeout)
    producer
  }
}

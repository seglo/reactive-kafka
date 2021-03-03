/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks.app

import akka.kafka.benchmarks.PerfFixtureHelpers.FilledTopic
import akka.kafka.testkit.internal.KafkaTestKit

case class RunTestCommand(testName: String, kafkaTestKit: KafkaTestKit, filledTopic: FilledTopic) {

  val msgCount = filledTopic.msgCount
  val msgSize = filledTopic.msgSize
  val numberOfPartitions = filledTopic.numberOfPartitions
  val replicationFactor = filledTopic.replicationFactor

}

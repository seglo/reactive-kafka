package sample.scaladsl

import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.Transactional
import akka.kafka.{ProducerMessage, Subscriptions}
import akka.stream.scaladsl.{RestartSource, Sink}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Await
import scala.concurrent.duration._

class TransactionsSink extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #transactionalSink
    val control =
      Transactional
        .source(consumerSettings, Subscriptions.topics("source-topic"))
        .via(business)
        .map { msg =>
          ProducerMessage.Message(new ProducerRecord[String, Array[Byte]]("sink-topic", msg.record.value),
                                  msg.partitionOffset)
        }
        .to(Transactional.sink(producerSettings, "transactional-id"))
        .run()

    // ...

    control.shutdown()
    // #transactionalSink
    terminateWhenDone(control.shutdown())
  }
}

class TransactionsFailureRetryExample extends ConsumerExample {
  def main(args: Array[String]): Unit = {
    // #transactionalFailureRetry
    var innerControl: Control = null

    val stream = RestartSource.onFailuresWithBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    ) { () =>
      Transactional
        .source(consumerSettings, Subscriptions.topics("source-topic"))
        .via(business)
        .map { msg =>
          ProducerMessage.Message(new ProducerRecord[String, Array[Byte]]("sink-topic", msg.record.value),
                                  msg.partitionOffset)
        }
        // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
        .mapMaterializedValue(innerControl = _)
        .via(Transactional.flow(producerSettings, "transactional-id"))
    }

    stream.runWith(Sink.ignore)

    // Add shutdown hook to respond to SIGTERM and gracefully shutdown stream
    sys.ShutdownHookThread {
      Await.result(innerControl.shutdown(), 10.seconds)
    }
    // #transactionalFailureRetry

    terminateWhenDone(innerControl.shutdown())
  }
}

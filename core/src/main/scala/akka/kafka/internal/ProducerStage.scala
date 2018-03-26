/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Map => JMap}

import akka.kafka.ConsumerMessage
import akka.kafka.ConsumerMessage.{GroupTopicPartition, PartitionOffset}
import akka.kafka.ProducerMessage.{Message, Result}
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.stage._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{Callback, Producer, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
private[kafka] class ProducerStage[K, V, P](
    closeTimeout: FiniteDuration, closeProducerOnStop: Boolean,
    eosEnabled: Boolean, eosCommitIntervalMs: Long,
    producerProvider: () => Producer[K, V]
) extends GraphStage[FlowShape[Message[K, V, P], Future[Result[K, V, P]]]] {

  private val in = Inlet[Message[K, V, P]]("messages")
  private val out = Outlet[Future[Result[K, V, P]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(_inheritedAttributes: Attributes) = {
    val _producer: Producer[K, V] = producerProvider()

    if (eosEnabled) {
      new TransactionProducerStageLogic(shape, _producer, _inheritedAttributes, eosCommitIntervalMs)
    }
    else {
      new ProducerStageLogic(shape, _producer, _inheritedAttributes)
    }
  }

  /**
   * Default Producer State Logic
   */
  class ProducerStageLogic(shape: Shape, producer: Producer[K, V], inheritedAttributes: Attributes) extends TimerGraphStageLogic(shape) with StageLogging with MessageCallback[K, V, P] with ProducerCompletionState {
    lazy val decider = inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
    val awaitingConfirmation = new AtomicInteger(0)
    @volatile var inIsClosed = false
    var completionState: Option[Try[Unit]] = None

    override protected def logSource: Class[_] = classOf[ProducerStage[K, V, P]]

    def checkForCompletion() = {
      if (isClosed(in) && awaitingConfirmation.get == 0) {
        completionState match {
          case Some(Success(_)) =>
            onCompletionSuccess()
            completeStage()
          case Some(Failure(ex)) =>
            onCompletionFailure(ex)
            failStage(ex)
          case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
        }
      }
    }

    val checkForCompletionCB = getAsyncCallback[Unit] { _ =>
      checkForCompletion()
    }

    val failStageCb = getAsyncCallback[Throwable] { ex =>
      failStage(ex)
    }

    override val onMessageAckCb: AsyncCallback[Message[K, V, P]] = getAsyncCallback[Message[K, V, P]] { _ => }

    setHandler(out, new OutHandler {
      override def onPull() = tryPull(in)
    })

    setHandler(in, new InHandler {
      override def onPush() = produce(grab(in))

      override def onUpstreamFinish() = {
        inIsClosed = true
        completionState = Some(Success(()))
        checkForCompletion()
      }

      override def onUpstreamFailure(ex: Throwable) = {
        inIsClosed = true
        completionState = Some(Failure(ex))
        checkForCompletion()
      }
    })

    def produce(msg: Message[K, V, P]): Unit = {
      val r = Promise[Result[K, V, P]]
      producer.send(msg.record, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
          if (exception == null) {
            onMessageAckCb.invoke(msg)
            r.success(Result(metadata, msg))
          }
          else {
            decider(exception) match {
              case Supervision.Stop =>
                if (closeProducerOnStop) {
                  producer.close(0, TimeUnit.MILLISECONDS)
                }
                failStageCb.invoke(exception)
              case _ =>
                r.failure(exception)
            }
          }

          if (awaitingConfirmation.decrementAndGet() == 0 && inIsClosed)
            checkForCompletionCB.invoke(())
        }
      })
      awaitingConfirmation.incrementAndGet()
      push(out, r.future)
    }

    override def postStop() = {
      log.debug("Stage completed")

      if (closeProducerOnStop) {
        try {
          // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
          producer.flush()
          producer.close(closeTimeout.toMillis, TimeUnit.MILLISECONDS)
          log.debug("Producer closed")
        }
        catch {
          case NonFatal(ex) => log.error(ex, "Problem occurred during producer close")
        }
      }

      super.postStop()
    }
  }

  /**
   * Transactional (Exactly-Once) Producer State Logic
   */
  class TransactionProducerStageLogic(shape: Shape, producer: Producer[K, V], inheritedAttributes: Attributes, commitIntervalMs: Long) extends ProducerStageLogic(shape, producer, inheritedAttributes) with StageLogging with MessageCallback[K, V, P] with ProducerCompletionState {
    private val commitSchedulerKey = "commit"
    private val messageDrainIntervalMs = 10

    private var isTransactionOpen = false
    private var commitInProgress = false
    private val batchOffsets = new TransactionOffsetBatch

    override def preStart(): Unit = {
      initTransactions()
      beginTransaction()
      scheduleOnce(commitSchedulerKey, commitIntervalMs.milliseconds)
      super.preStart()
    }

    setHandler(out, new OutHandler {
      override def onPull() = {
        // stop pulling while a commit is in process so we can drain any outstanding message acknowledgements
        if (!commitInProgress) {
          tryPull(in)
        }
      }
    })

    override protected def onTimer(timerKey: Any): Unit =
      if (timerKey == commitSchedulerKey) {
        onCommitInterval()
      }

    private def onCommitInterval(): Unit = {
      if (!commitInProgress) {
        commitInProgress = true
      }
      val awaitingConfirmationCount = awaitingConfirmation.get
      if (awaitingConfirmationCount == 0) {
        maybeCommitTransaction()
        commitInProgress = false
        // restart demand for more messages once last batch has been committed
        if (!hasBeenPulled(in)) {
          tryPull(in)
        }
        scheduleOnce(commitSchedulerKey, commitIntervalMs.milliseconds)
      }
      else {
        scheduleOnce(commitSchedulerKey, messageDrainIntervalMs.milliseconds)
      }
    }

    override val onMessageAckCb: AsyncCallback[Message[K, V, P]] = getAsyncCallback[Message[K, V, P]] { (msg) =>
      msg.passThrough match {
        case o: ConsumerMessage.PartitionOffset => batchOffsets.update(o)
        case _ =>
      }
    }

    override def onCompletionSuccess(): Unit = {
      log.debug("Committing final transaction before shutdown")
      maybeCommitTransaction(beginNewTransaction = false)
    }

    override def onCompletionFailure(ex: Throwable): Unit = {
      log.debug("Aborting transaction due to stage failure")
      abortTransaction()
    }

    private def maybeCommitTransaction(beginNewTransaction: Boolean = true): Unit = {
      if (batchOffsets.nonEmpty) {
        val (groupId, offsetMap) = batchOffsets.offsetMap()
        log.debug(s"Committing transaction for consumer group '${groupId}' with offsets: $offsetMap")
        producer.sendOffsetsToTransaction(offsetMap, groupId)
        producer.commitTransaction()
        batchOffsets.clear()
        isTransactionOpen = false
        if (beginNewTransaction) {
          beginTransaction()
        }
      }
    }

    private def initTransactions(): Unit = {
      log.debug("Initializing transactions")
      producer.initTransactions()
    }

    private def beginTransaction(): Unit = {
      log.debug("Beginning new transaction")
      producer.beginTransaction()
      isTransactionOpen = true
    }

    private def abortTransaction(): Unit = {
      log.debug("Aborting transaction")
      producer.abortTransaction()
      isTransactionOpen = false
    }
  }
}

private[kafka] trait ProducerCompletionState {
  def onCompletionSuccess(): Unit = {}
  def onCompletionFailure(ex: Throwable): Unit = {}
}

private[kafka] trait MessageCallback[K, V, P] {
  def awaitingConfirmation: AtomicInteger
  def onMessageAckCb: AsyncCallback[Message[K, V, P]]
}

private[kafka] class TransactionOffsetBatch() {
  private val offsets = mutable.Map[GroupTopicPartition, Long]()

  def update(partitionOffset: PartitionOffset): Unit = {
    val key = partitionOffset.key
    offsets.update(key, partitionOffset.offset)
  }

  def nonEmpty: Boolean = offsets.nonEmpty
  def clear(): Unit = offsets.clear()

  def offsetMap(): (String, JMap[TopicPartition, OffsetAndMetadata]) = {
    val groups = offsets.groupBy {
      case (groupTopicPartition, _) => groupTopicPartition.groupId
    }.keys

    require(groups.size == 1, "Transactional batch must contain messages from exactly 1 consumer group. " +
      s"Found: [${groups.mkString(",")}]")

    (groups.head, offsets.map {
      case (gtp, offset) => new TopicPartition(gtp.topic, gtp.partition) -> new OffsetAndMetadata(offset + 1)
    }.asJava)
  }
}

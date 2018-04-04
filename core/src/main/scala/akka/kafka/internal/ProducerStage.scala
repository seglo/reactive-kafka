/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

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
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * INTERNAL API
 */
private[kafka] object ProducerStage {

  class DefaultProducerStage[K, V, P](val closeTimeout: FiniteDuration, val closeProducerOnStop: Boolean,
      val producerProvider: () => Producer[K, V])
    extends GraphStage[FlowShape[Message[K, V, P], Future[Result[K, V, P]]]] with ProducerStage[K, V, P] {

    override def createLogic(inheritedAttributes: Attributes) =
      new DefaultProducerStageLogic(this, producerProvider(), inheritedAttributes)
  }

  class TransactionProducerStage[K, V, P](val closeTimeout: FiniteDuration, val closeProducerOnStop: Boolean,
      val producerProvider: () => Producer[K, V], commitInterval: Long)
    extends GraphStage[FlowShape[Message[K, V, P], Future[Result[K, V, P]]]] with ProducerStage[K, V, P] {

    override def createLogic(inheritedAttributes: Attributes) =
      new TransactionProducerStageLogic(this, producerProvider(), inheritedAttributes, commitInterval)
  }

  trait ProducerStage[K, V, P] {
    val closeTimeout: FiniteDuration
    val closeProducerOnStop: Boolean
    val producerProvider: () => Producer[K, V]

    val in = Inlet[Message[K, V, P]]("messages")
    val out = Outlet[Future[Result[K, V, P]]]("result")
    val shape = FlowShape(in, out)
  }

  /**
   * Default Producer State Logic
   */
  class DefaultProducerStageLogic[K, V, P](stage: ProducerStage[K, V, P], producer: Producer[K, V],
      inheritedAttributes: Attributes)
    extends TimerGraphStageLogic(stage.shape) with StageLogging with MessageCallback[K, V, P] with ProducerCompletionState {

    lazy val decider = inheritedAttributes.get[SupervisionStrategy].map(_.decider).getOrElse(Supervision.stoppingDecider)
    val awaitingConfirmation = new AtomicInteger(0)
    @volatile var inIsClosed = false
    var completionState: Option[Try[Unit]] = None

    override protected def logSource: Class[_] = classOf[DefaultProducerStage[_, _, _]]

    def checkForCompletion() = {
      if (isClosed(stage.in) && awaitingConfirmation.get == 0) {
        completionState match {
          case Some(Success(_)) => onCompletionSuccess()
          case Some(Failure(ex)) => onCompletionFailure(ex)
          case None => failStage(new IllegalStateException("Stage completed, but there is no info about status"))
        }
      }
    }

    override def onCompletionSuccess(): Unit = completeStage()

    override def onCompletionFailure(ex: Throwable): Unit = failStage(ex)

    val checkForCompletionCB = getAsyncCallback[Unit] { _ =>
      checkForCompletion()
    }

    val failStageCb = getAsyncCallback[Throwable] { ex =>
      failStage(ex)
    }

    override val onMessageAckCb: AsyncCallback[Message[K, V, P]] = getAsyncCallback[Message[K, V, P]] { _ => }

    setHandler(stage.out, new OutHandler {
      override def onPull() = tryPull(stage.in)
    })

    setHandler(stage.in, new InHandler {
      override def onPush() = produce(grab(stage.in))

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
                if (stage.closeProducerOnStop) {
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
      push(stage.out, r.future)
    }

    override def postStop() = {
      log.debug("Stage completed")

      if (stage.closeProducerOnStop) {
        try {
          // we do not have to check if producer was already closed in send-callback as `flush()` and `close()` are effectively no-ops in this case
          producer.flush()
          producer.close(stage.closeTimeout.toMillis, TimeUnit.MILLISECONDS)
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
   * Transaction (Exactly-Once) Producer State Logic
   */
  class TransactionProducerStageLogic[K, V, P](stage: ProducerStage[K, V, P], producer: Producer[K, V],
      inheritedAttributes: Attributes, commitIntervalMs: Long)
    extends DefaultProducerStageLogic(stage, producer, inheritedAttributes) with StageLogging with MessageCallback[K, V, P] with ProducerCompletionState {
    private val commitSchedulerKey = "commit"
    private val messageDrainIntervalMs = 10

    private var batchOffsets = TransactionOffsetBatch.empty

    override def preStart(): Unit = {
      initTransactions()
      beginTransaction()
      resumeDemand(tryToPull = false)
      scheduleOnce(commitSchedulerKey, commitIntervalMs.milliseconds)
    }

    private def resumeDemand(tryToPull: Boolean = true): Unit = {
      setHandler(stage.out, new OutHandler {
        override def onPull() = tryPull(stage.in)
      })
      // kick off demand for more messages if we're resuming demand
      if (tryToPull && !hasBeenPulled(stage.in)) {
        tryPull(stage.in)
      }
    }

    private def suspendDemand(): Unit = setHandler(stage.out, new OutHandler {
      // suspend demand while a commit is in process so we can drain any outstanding message acknowledgements
      override def onPull() = ()
    })

    override protected def onTimer(timerKey: Any): Unit =
      if (timerKey == commitSchedulerKey) {
        onCommitInterval()
      }

    private def onCommitInterval(): Unit = {

      suspendDemand()
      if (awaitingConfirmation.get == 0) {
        maybeCommitTransaction()
        resumeDemand()
        scheduleOnce(commitSchedulerKey, commitIntervalMs.milliseconds)
      }
      else {
        scheduleOnce(commitSchedulerKey, messageDrainIntervalMs.milliseconds)
      }
    }

    override val onMessageAckCb: AsyncCallback[Message[K, V, P]] =
      getAsyncCallback[Message[K, V, P]](_.passThrough match {
        case o: ConsumerMessage.PartitionOffset => batchOffsets = batchOffsets.updated(o)
        case _ =>
      })

    override def onCompletionSuccess(): Unit = {
      log.debug("Committing final transaction before shutdown")
      cancelTimer(commitSchedulerKey)
      maybeCommitTransaction(beginNewTransaction = false)
      super.onCompletionSuccess()
    }

    override def onCompletionFailure(ex: Throwable): Unit = {
      log.debug("Aborting transaction due to stage failure")
      abortTransaction()
      super.onCompletionFailure(ex)
    }

    private def maybeCommitTransaction(beginNewTransaction: Boolean = true): Unit = batchOffsets match {
      case batch: NonemptyTransactionOffsetBatch =>
        val group = batch.group
        val offsetMap = batch.offsetMap().asJava
        log.debug(s"Committing transaction for consumer group '$group' with offsets: $offsetMap")
        producer.sendOffsetsToTransaction(offsetMap, group)
        producer.commitTransaction()
        batchOffsets = TransactionOffsetBatch.empty
        if (beginNewTransaction) {
          beginTransaction()
        }
      case _ =>
    }

    private def initTransactions(): Unit = {
      log.debug("Initializing transactions")
      producer.initTransactions()
    }

    private def beginTransaction(): Unit = {
      log.debug("Beginning new transaction")
      producer.beginTransaction()
    }

    private def abortTransaction(): Unit = {
      log.debug("Aborting transaction")
      producer.abortTransaction()
    }
  }

  trait ProducerCompletionState {
    def onCompletionSuccess(): Unit
    def onCompletionFailure(ex: Throwable): Unit
  }

  trait MessageCallback[K, V, P] {
    def awaitingConfirmation: AtomicInteger
    def onMessageAckCb: AsyncCallback[Message[K, V, P]]
  }

  object TransactionOffsetBatch {
    def empty: TransactionOffsetBatch = new EmptyTransactionOffsetBatch()
  }

  trait TransactionOffsetBatch {
    def updated(partitionOffset: PartitionOffset): TransactionOffsetBatch
  }

  class EmptyTransactionOffsetBatch extends TransactionOffsetBatch {
    override def updated(partitionOffset: PartitionOffset): TransactionOffsetBatch = new NonemptyTransactionOffsetBatch(partitionOffset)
  }

  class NonemptyTransactionOffsetBatch(
      head: PartitionOffset,
      tail: Map[GroupTopicPartition, Long] = Map[GroupTopicPartition, Long]())
    extends TransactionOffsetBatch {
    private val offsets = tail + (head.key -> head.offset)

    def group: String = offsets.keys.head.groupId
    override def updated(partitionOffset: PartitionOffset): TransactionOffsetBatch = {
      require(
        group == partitionOffset.key.groupId,
        s"Transaction batch must contain messages from exactly 1 consumer group. $group != ${partitionOffset.key.groupId}")
      new NonemptyTransactionOffsetBatch(partitionOffset, offsets)
    }
    def offsetMap(): Map[TopicPartition, OffsetAndMetadata] =
      offsets.map {
        case (gtp, offset) => new TopicPartition(gtp.topic, gtp.partition) -> new OffsetAndMetadata(offset + 1)
      }
  }

}

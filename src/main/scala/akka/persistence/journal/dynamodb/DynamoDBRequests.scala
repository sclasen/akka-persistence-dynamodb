package akka.persistence.journal.dynamodb

import java.util.{List => JList, Map => JMap}

import akka.persistence.journal.dynamodb.DynamoDBJournal._
import akka.persistence.{PersistentRepr, AtomicWrite}
import com.amazonaws.services.dynamodbv2.model._

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

trait DynamoDBRequests {
  this: DynamoDBJournal =>

  /**
   * Writes all PersistentRepr in the AtomicWrite provided
   *
   * If there are any errors serializing (preparing the batch writes), then we must return
   * a Future.success(Failure) as the result.  This is needed to be compliant with
   * Akka Persistence 2.4
   *
   * @param atomicWrite Contains a list of persistentRepr that need to be persisted atomically
   * @return [[Future[Try[Unit]] - This is important, we always must return a result, even on failure
   */
  def writeMessages(atomicWrite: AtomicWrite): Future[Try[Unit]] = {

    // groups messages into groups of 12, a BatchWriteItemRequest has a max of 25 items, and
    // each PersistentRepr has to writes, the most we can put in any one request is 12 PersistentRepr
    val writes = Try { atomicWrite.payload.grouped(12).map(toBatchWriteItemRequest).toSeq }

    writes match {
      case Success(batchWrites) =>
        // we created our batch writes successfully, send them off to Dynamo
        val futures = writes.get.map {
          write =>

            // Dynamo can partially fail a number of write items in a batch write, usually because
            // of throughput constraints.  We need to continue to retry the unprocessed item
            // until we exhaust our backoff
            batchWrite(write).flatMap(r => sendUnprocessedItems(r)).map {
              _ =>
                if (log.isDebugEnabled)
                  log.debug("at=batch-write-finish writes={}", write.getRequestItems.get(journalTable).size())
                else ()
            }
        }

        // Squash all of the futures into a single result
        Future.sequence(futures).map { _ => Try() }

      case Failure(e) =>
        log.error(e, "DynamoDB Journal encountered error creating writes")

        // Note: In akka 2.4, asyncWriteMessages takes a Seq[AtomicWrite].  Akka assumes that you will
        // return a result for Every item in the Sequence (the result is a Future[Seq[Try[Unit]]];
        // therefore, we should not explicitly fail any given future due to serialization, but rather
        // we should "Reject" it, meaning we return successfully with a Failure
        Future.successful(Failure(e))
    }
  }

  /**
   * Converts a sequence of PersistentRepr to a single batch write request
   */
  private[dynamodb] def toBatchWriteItemRequest(msgs:Seq[PersistentRepr]):BatchWriteItemRequest = {
    val writes = msgs.foldLeft(new mutable.ArrayBuffer[WriteRequest](msgs.length)) {
      case (ws, repr) =>
        ws += putReq(toMsgItem(repr))
        ws += putReq(toHSItem(repr))
        ws
    }
    val reqItems = fields(journalTable -> writes.asJava)
    batchWriteReq(reqItems)
  }

  /**
   * Sends the unprocessed batch write items, and sets the back-off.
   * if no more retries remain (number of back-off retries exhausted, we throw a Runtime exception
   *
   * Note: the dynamo db client supports automatic retries, however a batch will not fail if some of the items in the
   * batch fail; that is why we need our own back-off mechanism here.  If we exhaust OUR retry logic on top of
   * the retries from teh client, then we are hosed and cannot continue; that is why we have a RuntimeException here
   */
  private[dynamodb] def sendUnprocessedItems(result: BatchWriteItemResult,
    retriesRemaining: Int = 10): Future[BatchWriteItemResult] = {

    val unprocessed: Int = Option(result.getUnprocessedItems.get(JournalTable)).map(_.size()).getOrElse(0)
    if (unprocessed == 0) Future.successful(result)
    else if (retriesRemaining == 0) {
      throw new RuntimeException(s"unable to batch write $result after 10 tries")
    } else {
      log.warning("at=unprocessed-writes unprocessed={}", unprocessed)
      backoff(10 - retriesRemaining, classOf[BatchWriteItemRequest].getSimpleName)
      val rest = batchWriteReq(result.getUnprocessedItems)
      batchWrite(rest, retriesRemaining - 1).flatMap(r => sendUnprocessedItems(r, retriesRemaining - 1))
    }
  }

  def putItem(r: PutItemRequest): Future[PutItemResult] = withBackoff(r)(dynamo.putItem)

  def deleteItem(r: DeleteItemRequest): Future[DeleteItemResult] = withBackoff(r)(dynamo.deleteItem)

  def updateItem(r: UpdateItemRequest): Future[UpdateItemResult] = withBackoff(r)(dynamo.updateItem)

  def batchWrite(r: BatchWriteItemRequest, retriesRemaining: Int = 10): Future[BatchWriteItemResult] =
    withBackoff(r, retriesRemaining)(dynamo.batchWriteItem)

  def deleteMessages(persistenceId: String, sequenceNrs: immutable.Seq[Long]): Future[Unit] = unitSequence {
    sequenceNrs.map {
      sequenceNr =>
        deleteItem(permanentDeleteToDelete(persistenceId, sequenceNr)).map {
          _ => log.debug("at=permanent-delete-item  processorId={} sequenceId={}", persistenceId, sequenceNr)
        }.flatMap {
          _ =>
            val item = toLSItem(persistenceId, sequenceNr)
            val put = new PutItemRequest().withTableName(journalTable).withItem(item)
            putItem(put).map(
              _ => log.debug("at=update-sequence-low-shard processorId={} sequenceId={}", persistenceId, sequenceNr))
        }
    }
  }

  def toMsgItem(repr: PersistentRepr): Item = fields(
    Key -> messageKey(repr.persistenceId, repr.sequenceNr),
    Payload -> B(serialization.serialize(repr).get),
    Deleted -> S(false)
  )

  def toHSItem(repr: PersistentRepr): Item = fields(
    Key -> highSeqKey(repr.persistenceId, repr.sequenceNr % sequenceShards),
    SequenceNr -> N(repr.sequenceNr)
  )

  def toLSItem(persistenceId: String, sequenceNr: Long): Item = fields(
    Key -> lowSeqKey(persistenceId, sequenceNr % sequenceShards),
    SequenceNr -> N(sequenceNr)
  )

  def putReq(item: Item): WriteRequest = new WriteRequest().withPutRequest(new PutRequest().withItem(item))

  def deleteReq(item: Item): WriteRequest = new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(item))

  def updateReq(key: Item, updates: ItemUpdates): UpdateItemRequest = new UpdateItemRequest()
    .withTableName(journalTable)
    .withKey(key)
    .withAttributeUpdates(updates)
    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

  def setAdd(value: AttributeValue): AttributeValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.ADD)
    .withValue(value)

  def batchWriteReq(items: JMap[String, JList[WriteRequest]]) = new BatchWriteItemRequest()
    .withRequestItems(items)
    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

  def permanentDeleteToDelete(persistenceId: String, sequenceNr: Long): DeleteItemRequest = {
    log.debug("delete permanent {}", sequenceNr)
    val key = fields(Key -> messageKey(persistenceId, sequenceNr))
    new DeleteItemRequest().withTableName(journalTable).withKey(key)
  }

  def unitSequence(seq: TraversableOnce[Future[Unit]]): Future[Unit] = Future.sequence(seq).map(_ => ())
}


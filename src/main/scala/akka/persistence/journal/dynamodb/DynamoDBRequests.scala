package akka.persistence.journal.dynamodb

import java.util.{HashMap => JHMap, List => JList, Map => JMap}

import akka.persistence.PersistentRepr
import com.amazonaws.services.dynamodbv2.model._
import akka.persistence.journal.dynamodb.DynamoDBJournal._
import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.concurrent.Future


trait DynamoDBRequests {
  this: DynamoDBJournal =>

  def writeMessages(messages: immutable.Seq[PersistentRepr]): Future[Unit] = unitSequence {
    // 25 is max items so group by 12 since 2 items per message
    // todo size calculation < 1M
    val writes = messages.grouped(12).map {
      msgs =>
        val writes = msgs.foldLeft(new mutable.ArrayBuffer[WriteRequest](messages.length)) {
          case (ws, repr) =>
            ws += putReq(toMsgItem(repr))
            ws += putReq(toHSItem(repr))
            ws
        }
        val reqItems = fields(journalTable -> writes.asJava)
        batchWriteReq(reqItems)
    }

    writes.map {
      write =>
        batchWrite(write).flatMap(r => sendUnprocessedItems(r)).map {
          _ => if (log.isDebugEnabled) {
            log.debug("at=batch-write-finish writes={}", write.getRequestItems.get(journalTable).size())
          } else ()
        }
    }

  }

  private[dynamodb] def sendUnprocessedItems(result: BatchWriteItemResult, retriesRemaining: Int = 10): Future[BatchWriteItemResult] = {
    val unprocessed: Int = Option(result.getUnprocessedItems.get(JournalTable)).map(_.size()).getOrElse(0)
    if (unprocessed == 0) Future.successful(result)
    else if (retriesRemaining == 0) {
      throw new RuntimeException(s"unable to batch write ${result} after 10 tries")
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

  def batchWrite(r: BatchWriteItemRequest, retriesRemaining: Int = 10): Future[BatchWriteItemResult] = withBackoff(r, retriesRemaining)(dynamo.batchWriteItem)

  def deleteMessages(messageIds: immutable.Seq[PersistentKey], permanent: Boolean): Future[Unit] = unitSequence {
    messageIds.map {
      msg =>
        if (permanent) {
          deleteItem(permanentDeleteToDelete(msg)).map {
            _ => log.debug("at=permanent-delete-item  processorId={} sequenceId={}", msg.persistenceId, msg.sequenceNr)
          }
        } else {
          updateItem(impermanentDeleteToUpdate(msg)).map {
            _ => log.debug("at=mark-delete-item  processorId={} sequenceId={}", msg.persistenceId, msg.sequenceNr)
          }
        }.flatMap {
          _ =>
            val item = toLSItem(msg)
            val put = new PutItemRequest().withTableName(journalTable).withItem(item)
            putItem(put).map(_ => log.debug("at=update-sequence-low-shard processorId={} sequenceId={}", msg.persistenceId, msg.sequenceNr))
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

  def toLSItem(id: PersistentKey): Item = fields(
    Key -> lowSeqKey(id.persistenceId, id.sequenceNr % sequenceShards),
    SequenceNr -> N(id.sequenceNr)
  )

  def putReq(item: Item): WriteRequest = new WriteRequest().withPutRequest(new PutRequest().withItem(item))

  def deleteReq(item: Item): WriteRequest = new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(item))

  def updateReq(key: Item, updates: ItemUpdates): UpdateItemRequest = new UpdateItemRequest()
    .withTableName(journalTable)
    .withKey(key)
    .withAttributeUpdates(updates)
    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

  def setAdd(value: AttributeValue): AttributeValueUpdate = new AttributeValueUpdate().withAction(AttributeAction.ADD).withValue(value)

  def batchWriteReq(items: JMap[String, JList[WriteRequest]]) = new BatchWriteItemRequest()
    .withRequestItems(items)
    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

  def permanentDeleteToDelete(id: PersistentKey): DeleteItemRequest = {
    log.debug("delete permanent {}", id)
    val key = fields(Key -> messageKey(id.persistenceId, id.sequenceNr))
    new DeleteItemRequest().withTableName(journalTable).withKey(key)
  }

  def impermanentDeleteToUpdate(id: PersistentKey): UpdateItemRequest = {
    log.debug("delete {}", id)
    val key = fields(Key -> messageKey(id.persistenceId, id.sequenceNr))
    val updates = fields(Deleted -> new AttributeValueUpdate().withAction(AttributeAction.PUT).withValue(S(true)))
    new UpdateItemRequest().withTableName(journalTable).withKey(key).withAttributeUpdates(updates)
  }

  def unitSequence(seq: TraversableOnce[Future[Unit]]): Future[Unit] = Future.sequence(seq).map(_ => ())

}

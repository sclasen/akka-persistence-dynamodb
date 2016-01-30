package akka.persistence.journal.dynamodb

import java.util.{Collections, HashMap => JHMap, List => JList, Map => JMap}

import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery
import akka.persistence.journal.dynamodb.DynamoDBJournal._
import com.amazonaws.services.dynamodbv2.model._

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.{Stream => ScalaStream}

trait DynamoDBRecovery extends AsyncRecovery {
  this: DynamoDBJournal =>

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(config.getString(ReplayDispatcher))

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
    (replayCallback: (PersistentRepr) => Unit): Future[Unit] = logging {
    if (fromSequenceNr > toSequenceNr) return Future.successful(())
    var delivered = 0L
    var maxDeliveredSeq = 0L
    getReplayBatch(persistenceId, fromSequenceNr).map {
      replayBatch =>
        replayBatch.keys.foreach {
          case (sequenceNr, key) =>
            val k = key.get(Key)
            Option(replayBatch.batch.get(k)).foreach {
              item =>
                val repr = readPersistentRepr(item)
                repr.foreach {
                  r =>
                    if (delivered < max && maxDeliveredSeq < toSequenceNr) {
                      replayCallback(r)
                      delivered += 1
                      maxDeliveredSeq = r.sequenceNr
                      log.debug("in=replay at=deliver {} {}", persistenceId, sequenceNr)
                    }
                }
            }
        }
        replayBatch.batch.size()
    }.flatMap {
      last =>
        if (last < maxDynamoBatchGet * replayParallelism || delivered >= max || maxDeliveredSeq >= toSequenceNr) {
          Future.successful(())
        } else {
          val from = fromSequenceNr + maxDynamoBatchGet * replayParallelism
          asyncReplayMessages(persistenceId, from, toSequenceNr, max - delivered)(replayCallback)
        }
    }
  }

  def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("in=read-highest processorId={} from={}", processorId, fromSequenceNr)
    Future.sequence {
      ScalaStream.iterate(0L, sequenceShards)(_ + 1).map(l => highSeqKey(processorId, l)).grouped(100).map {
        keys =>
          val keyColl = keys.map(k => fields(Key -> k)).toSeq.asJava
          val ka = new KeysAndAttributes().withKeys(keyColl).withConsistentRead(true)
          val get = batchGetReq(Collections.singletonMap(journalTable, ka))
          log.debug("in=read-highest at=batch-request")
          batchGet(get).flatMap(r => getUnprocessedItems(r)).map {
            resp =>
              log.debug("in=read-highest at=batch-response")
              val batchMap = mapBatch(resp.getResponses.get(journalTable))
              keys.flatMap { key =>
                Option(batchMap.get(key)).map(item => item.get(SequenceNr).getN.toLong)
              }.append(ScalaStream(0L)).max
          }
      }
    }.map(_.max).map {
      max =>
        log.debug("at=finish-read-high-sequence high={}", max)
        max
    }
  }

  case class ReplayBatch(keys: ScalaStream[(Long, Item)], batch: JMap[AttributeValue, Item])

  def getReplayBatch(processorId: String, fromSequenceNr: Long): Future[ReplayBatch] = {
    val batchKeys = ScalaStream.iterate(fromSequenceNr, maxDynamoBatchGet * replayParallelism)(_ + 1)
      .map(s => s -> fields(Key -> messageKey(processorId, s)))
    //there will be replayParallelism number of gets
    val gets = batchKeys.grouped(maxDynamoBatchGet).map {
      keys =>
        val ka = new KeysAndAttributes().withKeys(keys.map(_._2).asJava).withConsistentRead(true)
          .withAttributesToGet(Key, Payload, Deleted, Confirmations)
        val get = batchGetReq(Collections.singletonMap(journalTable, ka))
        batchGet(get).flatMap(r => getUnprocessedItems(r)).map {
          result => mapBatch(result.getResponses.get(journalTable))
        }
    }

    Future.sequence(gets).map {
      responses =>
        val batch = responses.foldLeft(mapBatch(Collections.emptyList())) {
          case (all, one) =>
            all.putAll(one)
            all
        }
        ReplayBatch(batchKeys, batch)
    }
  }

  def readPersistentRepr(item: JMap[String, AttributeValue]): Option[PersistentRepr] = {
    Option(item.get(Payload)).map {
      payload =>
        val repr = persistentFromByteBuffer(payload.getB)
        val isDeleted = item.get(Deleted).getS == "true"
        val confirmations = item.asScala.get(Confirmations).map { ca =>
          ca.getSS.asScala.to[immutable.Seq]
        }.getOrElse(immutable.Seq[String]())

        // TODO: WHERE DID CONFIRMATIONS GO?
        repr.update(deleted = isDeleted)
    }
  }

  def readLowestSequenceNr(processorId: String): Future[Long] = {
    log.debug("at=read-lowest-sequence processorId={}", processorId)
    Future.sequence {
      ScalaStream.iterate(0L, sequenceShards)(_ + 1).map(l => lowSeqKey(processorId, l)).grouped(100).map {
        keys =>
          val keyColl = keys.map(k => fields(Key -> k)).toSeq.asJava
          val ka = new KeysAndAttributes().withKeys(keyColl).withConsistentRead(true)
          val get = batchGetReq(Collections.singletonMap(journalTable, ka))
          batchGet(get).flatMap(r => getUnprocessedItems(r)).map {
            resp =>
              log.debug("at=read-lowest-sequence-batch-response processorId={}", processorId)
              val batchMap = mapBatch(resp.getResponses.get(journalTable))
              val min: Long = keys.flatMap { key =>
                Option(batchMap.get(key)).map(item => item.get(SequenceNr).getN.toLong)
              }.append(ScalaStream(Long.MaxValue)).min
              min
          }
      }
    }.map(_.min).map {
      min =>
        log.debug("at=finish-read-lowest lowest={}", min)
        if (min == Long.MaxValue) 0
        else min
    }
  }

  def getUnprocessedItems(result: BatchGetItemResult, retriesRemaining: Int = 10): Future[BatchGetItemResult] = {
    val unprocessed = Option(result.getUnprocessedKeys.get(journalTable)).map(_.getKeys.size()).getOrElse(0)
    if (unprocessed == 0) Future.successful(result)
    else if (retriesRemaining == 0) {
      throw new RuntimeException(s"unable to batch get $result after 10 tries")
    } else {
      log.warning("at=unprocessed-reads, unprocessed={}", unprocessed)
      backoff(10 - retriesRemaining, classOf[BatchGetItemRequest].getSimpleName)
      val rest = batchGetReq(result.getUnprocessedKeys)
      batchGet(rest, retriesRemaining - 1).map {
        rr =>
          val items = rr.getResponses.get(journalTable)
          val responses = result.getResponses.get(journalTable)
          items.asScala.foreach {
            i => responses.add(i)
          }
          result
      }
    }
  }

  def batchGet(r: BatchGetItemRequest, retriesRemaining: Int = 10): Future[BatchGetItemResult] =
    withBackoff(r, retriesRemaining)(dynamo.batchGetItem)

  def batchGetReq(items: JMap[String, KeysAndAttributes]) = new BatchGetItemRequest()
    .withRequestItems(items)
    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)

  def mapBatch(b: JList[Item]): JMap[AttributeValue, Item] = {
    val map = new JHMap[AttributeValue, JMap[String, AttributeValue]]
    b.asScala.foreach {
      item => map.put(item.get(Key), item)
    }
    map
  }
}



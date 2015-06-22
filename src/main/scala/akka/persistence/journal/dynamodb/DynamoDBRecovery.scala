package akka.persistence.journal.dynamodb

import DynamoDBJournal._
import akka.persistence.PersistentRepr
import akka.persistence.journal.AsyncRecovery
import collection.JavaConverters._
import collection.immutable
import com.amazonaws.services.dynamodbv2.model._
import java.util.Collections
import java.util.{HashMap => JHMap, Map => JMap, List => JList}
import scala.concurrent.Future


trait DynamoDBRecovery extends AsyncRecovery {
  this: DynamoDBJournal =>

  implicit lazy val replayDispatcher = context.system.dispatchers.lookup(config.getString(ReplayDispatcher))

  def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = logging {
    log.debug(s"in=replay akka wanted: for=$processorId, from=$fromSequenceNr, to=$toSequenceNr, max=$max")
    if (fromSequenceNr > toSequenceNr) return Future.successful(())
    var delivered = 0L
    var maxDeliveredSeq = 0L
    var looped = 0L
    asyncReadHighestSequenceNr(processorId, fromSequenceNr).flatMap {
      highSeqNr =>
        log.debug(s"in=replay for=$processorId, highSeqNr=$highSeqNr")
        if (fromSequenceNr > highSeqNr) {
          log.warning(s"in=replay for=$processorId. requested min=$fromSequenceNr > actual max=$highSeqNr")
          return Future.successful(())
        }
        readLowestSequenceNr(processorId).flatMap {
          lowSeqNr =>
            val calculatedLowSeqNr = lowSeqNr.max(fromSequenceNr)
            log.debug(s"in=replay for=$processorId, lowSeqNr=$lowSeqNr, recalculatedLowSeqNr=$calculatedLowSeqNr")
            log.debug("in=replay for={}, from={}, to={}", processorId, lowSeqNr, highSeqNr)
            getReplayBatch(processorId, calculatedLowSeqNr, highSeqNr).map {
              replayBatch =>
                replayBatch.keys.foreach {
                  case (sequenceNr, key) =>
                    val k = key.get(Key)
                    Option(replayBatch.batch.get(k)).map {
                      item =>
                        val repr = readPersistentRepr(item)
                        repr.foreach {
                          r =>
                            if (delivered < max && maxDeliveredSeq < highSeqNr) {
                              replayCallback(r)
                              delivered += 1
                              maxDeliveredSeq = r.sequenceNr
                              log.debug("in=replay at=deliver {} {}", processorId, sequenceNr)
                            }
                        }
                    }
                }
                replayBatch.batch.size()
            }.flatMap {
              last =>
                log.debug(s"in=replay for=$processorId, finalizing, last=$last")
                if (last < maxDynamoBatchGet * replayParallelism || delivered >= max || maxDeliveredSeq >= highSeqNr) {
                  log.debug("in=replay done. success.")
                  Future.successful(())
                } else {
                  log.debug(s"**** in=replay rewindow, for=$processorId, last=$last, delivered=$delivered, max=$max, maxDeliveredSeq=$maxDeliveredSeq, highSeqNr=$highSeqNr")
                  val from = fromSequenceNr + maxDynamoBatchGet * replayParallelism
                  asyncReplayMessages(processorId, from, toSequenceNr, max - delivered)(replayCallback)
                }
            }
        }
    }
  }

  case class ReplayBatch(keys: Stream[(Long, Item)], batch: JMap[AttributeValue, Item])

  def getReplayBatch(processorId: String, fromSequenceNr: Long, toSequenceNr: Long): Future[ReplayBatch] = {
    val batchSize = Math.min(toSequenceNr - fromSequenceNr + 1, maxDynamoBatchGet * replayParallelism)
    val batchKeys = Stream.iterate(fromSequenceNr, batchSize.toInt)(_ + 1).map(s => s -> fields(Key -> messageKey(processorId, s)))
    //there will be replayParallelism number of gets
    val gets = batchKeys.grouped(maxDynamoBatchGet).map {
      keys =>
        val ka = new KeysAndAttributes().withKeys(keys.map(_._2).asJava).withConsistentRead(true).withAttributesToGet(Key, Payload, Deleted, Confirmations)
        val get = batchGetReq(Collections.singletonMap(journalTable, ka))
        log.debug(s"in=replaybatch for=$processorId, batch=$get")
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
        val confirmations = item.asScala.get(Confirmations).map {
          ca => ca.getSS.asScala.to[immutable.Seq]
        }.getOrElse(immutable.Seq[String]())
        repr.update(deleted = isDeleted, confirms = confirmations)
    }
  }

  def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("in=read-highest processorId={} from={}", processorId, fromSequenceNr)
    Future.sequence {
      Stream.iterate(0L, sequenceShards)(_ + 1).map(l => highSeqKey(processorId, l)).grouped(100).map {
        keys =>
          val keyColl = keys.map(k => fields(Key -> k)).toSeq.asJava
          val ka = new KeysAndAttributes().withKeys(keyColl).withConsistentRead(true)
          val get = batchGetReq(Collections.singletonMap(journalTable, ka))
          log.debug("in=read-highest at=batch-request")
          batchGet(get).flatMap(r => getUnprocessedItems(r)).map {
            resp =>
              log.debug("in=read-highest at=batch-response")
              val batchMap = mapBatch(resp.getResponses.get(journalTable))
              keys.map {
                key =>
                  Option(batchMap.get(key)).map(item => item.get(SequenceNr).getN.toLong)
              }.flatten.append(Stream(0L)).max
          }
      }
    }.map(_.max).map {
      max =>
        log.debug("at=finish-read-high-sequence high={}", max)
        max
    }
  }

  def readLowestSequenceNr(processorId: String): Future[Long] = {
    log.debug("at=read-lowest-sequence processorId={}", processorId)
    Future.sequence {
      Stream.iterate(0L, sequenceShards)(_ + 1).map(l => lowSeqKey(processorId, l)).grouped(100).map {
        keys =>
          val keyColl = keys.map(k => fields(Key -> k)).toSeq.asJava
          val ka = new KeysAndAttributes().withKeys(keyColl).withConsistentRead(true)
          val get = batchGetReq(Collections.singletonMap(journalTable, ka))
          batchGet(get).flatMap(r => getUnprocessedItems(r)).map {
            resp =>
              log.debug("at=read-lowest-sequence-batch-response processorId={}", processorId)
              val batchMap = mapBatch(resp.getResponses.get(journalTable))
              val min: Long = keys.map {
                key =>
                  Option(batchMap.get(key)).map(item => item.get(SequenceNr).getN.toLong)
              }.flatten.append(Stream(Long.MaxValue)).min
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
      throw new RuntimeException(s"unable to batch get ${result} after 10 tries")
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

  def batchGet(r: BatchGetItemRequest, retriesRemaining: Int = 10): Future[BatchGetItemResult] = withBackoff(r, retriesRemaining)(dynamo.batchGetItem)

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



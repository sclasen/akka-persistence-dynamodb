package akka.persistence.journal.dynamodb

import java.util.UUID

import akka.actor._
import akka.persistence._
import akka.persistence.journal.dynamodb.DynamoDBJournal._
import akka.testkit._
import com.amazonaws.services.dynamodbv2.model.{CreateTableRequest, DeleteTableRequest, ListTablesRequest, ProvisionedThroughput}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * This class is pulled from https://github.com/krasserm/akka-persistence-cassandra/
 * @author https://github.com/krasserm
 *
 * I removed the snapshot parts of the test, but most of the rest belongs to Martin Krasser
 */
object DynamoDBIntegrationLoadSpec {

  def config: Config = {
    ConfigFactory.load(ActorSystem.findClassLoader())
      .withValue("dynamodb-journal.journal-name", ConfigValueFactory.fromAnyRef(System.currentTimeMillis().toString))
  }

  case class DeleteTo(snr: Long)

  class ProcessorAtomic(val persistenceId: String, receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
      case payload: List[_] =>
        persistAll(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        receiver ! payload
        receiver ! lastSequenceNr
        receiver ! recoveryRunning
    }
  }

  class ProcessorA(val persistenceId: String, receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
      case payload: String =>
        persist(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        receiver ! payload
        receiver ! lastSequenceNr
        receiver ! recoveryRunning
    }
  }

  class ProcessorC(val persistenceId: String, probe: ActorRef) extends PersistentActor {
    var last: String = _

    def receiveRecover: Receive = {
      case SnapshotOffer(_, snapshot: String) =>
        last = snapshot
        probe ! s"offered-${last}"
      case payload: String =>
        handle(payload)
    }

    def receiveCommand: Receive = {
      case "snap" =>
        saveSnapshot(last)
      case SaveSnapshotSuccess(_) =>
        probe ! s"snapped-${last}"
      case payload: String =>
        persist(payload)(handle)
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
    }

    def handle: Receive = {
      case payload: String =>
        last = s"$payload-$lastSequenceNr"
        probe ! s"updated-$last"
    }
  }

  class ProcessorCNoRecover(override val persistenceId: String, probe: ActorRef, recoverConfig: Recovery)
    extends ProcessorC(persistenceId, probe) {
    override def recovery = recoverConfig

    override def preStart() = ()
  }

  class ViewA(val viewId: String, val persistenceId: String, probe: ActorRef) extends PersistentView {
    def receive = {
      case payload =>
        probe ! payload
    }

    override def autoUpdate: Boolean = false

    override def autoUpdateReplayMax: Long = 0
  }
}

import akka.persistence.journal.dynamodb.DynamoDBIntegrationLoadSpec._

class Listener extends Actor {
  def receive = {
    case d: DeadLetter => println(d)
  }
}

class DynamoDBIntegrationLoadSpec
  extends TestKit(ActorSystem("test", config))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val config = system.settings.config.getConfig(Conf)
    val table = "integrationLoadSpec"
    val client = dynamoClient(system, system, config)
    val create = new CreateTableRequest()
      .withTableName(table)
      .withKeySchema(DynamoDBJournal.schema)
      .withAttributeDefinitions(DynamoDBJournal.schemaAttributes)
      .withProvisionedThroughput(new ProvisionedThroughput(10L, 10L))
    import system.dispatcher

    val setup = client.sendListTables(new ListTablesRequest()).flatMap {
      list =>
        if (list.getTableNames.size() > 0) {
          client.sendDeleteTable(new DeleteTableRequest(table)).flatMap {
            res =>
              client.sendCreateTable(create).map(_ => ())
          }
        } else {
          client.sendCreateTable(create).map(_ => ())
        }
    }
    Await.result(setup, 5 seconds)
  }

  def subscribeToRangeDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.DeleteMessagesTo])

  def awaitRangeDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.DeleteMessagesTo]

  def testRangeDelete(persistenceId: String): Unit = {
    val deleteProbe = TestProbe()
    subscribeToRangeDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    1L to 16L foreach { i =>
      processor1 ! s"a-${i}"
      expectMsgAllOf(s"a-${i}", i, false)
    }

    processor1 ! DeleteTo(3L)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    4L to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }

    processor1 ! DeleteTo(7L)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    8L to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }
  }

  "A DynamoDB journal" should {
    "write and replay messages" in {
      val persistenceId = UUID.randomUUID().toString
      val processor1 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self), "p1")
      1L to 16L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self), "p2")
      1L to 16L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }

      processor2 ! "b"
      expectMsgAllOf("b", 17L, false)
    }
    "not replay range-deleted messages" in {
      val persistenceId = UUID.randomUUID().toString
      testRangeDelete(persistenceId)
    }
    "replay messages incrementally" in {
      val persistenceId = UUID.randomUUID().toString
      val probe = TestProbe()
      val processor1 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
      1L to 6L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val view = system.actorOf(Props(classOf[ViewA], "p7-view", persistenceId, probe.ref))
      probe.expectNoMsg(200.millis)

      view ! Update(await = true, replayMax = 3L)
      probe.expectMsg(s"a-1")
      probe.expectMsg(s"a-2")
      probe.expectMsg(s"a-3")
      probe.expectNoMsg(200.millis)

      view ! Update(await = true, replayMax = 3L)
      probe.expectMsg(s"a-4")
      probe.expectMsg(s"a-5")
      probe.expectMsg(s"a-6")
      probe.expectNoMsg(200.millis)
    }
    "write and replay with persistAll greater than partition size skipping whole partition" in {
      val persistenceId = UUID.randomUUID().toString
      val probe = TestProbe()
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3", "a-4", "a-5", "a-6")
      1L to 6L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val testProbe = TestProbe()
      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, testProbe.ref))
      1L to 6L foreach { i =>
        testProbe.expectMsgAllOf(s"a-${i}", i, true)
      }
    }
    "write and replay with persistAll greater than partition size skipping part of a partition" in {
      val persistenceId = UUID.randomUUID().toString
      val probe = TestProbe()
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3")
      1L to 3L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      processorAtomic ! List("a-4", "a-5", "a-6")
      4L to 6L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val testProbe = TestProbe()
      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, testProbe.ref))
      1L to 6L foreach { i =>
        testProbe.expectMsgAllOf(s"a-${i}", i, true)
      }
    }
    "write and replay with persistAll less than partition size" in {
      val persistenceId = UUID.randomUUID().toString
      val probe = TestProbe()
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3", "a-4")
      1L to 4L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))
      1L to 4L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }
    }
    "not replay messages deleted from the +1 partition" in {
      val persistenceId = UUID.randomUUID().toString
      val probe = TestProbe()
      val deleteProbe = TestProbe()
      subscribeToRangeDeletion(deleteProbe)
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3", "a-4", "a-5", "a-6")
      1L to 6L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }
      processorAtomic ! DeleteTo(5L)
      awaitRangeDeletion(deleteProbe)

      val testProbe = TestProbe()
      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, testProbe.ref))
      testProbe.expectMsgAllOf(s"a-6", 6, true)
    }
  }
}
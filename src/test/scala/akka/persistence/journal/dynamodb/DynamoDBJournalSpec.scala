package akka.persistence.journal.dynamodb

import akka.actor.ActorSystem
import akka.persistence.journal.{JournalSpec, JournalPerfSpec}
import akka.persistence.journal.dynamodb.DynamoDBJournal._
import com.amazonaws.services.dynamodbv2.model._
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.concurrent.Await
import scala.concurrent.duration._

trait DynamoDBSpec extends BeforeAndAfterEach {
  this: Suite =>

  val system: ActorSystem

  override def beforeEach(): Unit = {
    val config = system.settings.config.getConfig(Conf)
    val table = config.getString(JournalTable)
    val client = dynamoClient(system, system, config)
    val create = new CreateTableRequest()
      .withTableName(table)
      .withKeySchema(DynamoDBJournal.schema)
      .withAttributeDefinitions(DynamoDBJournal.schemaAttributes)
      .withProvisionedThroughput(new ProvisionedThroughput()
      .withReadCapacityUnits(10000L)
      .withWriteCapacityUnits(10000L))
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
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
  }
}

class DynamoDBJournalSpec extends JournalSpec(ConfigFactory.load()) with DynamoDBSpec
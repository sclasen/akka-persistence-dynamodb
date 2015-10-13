package akka.persistence.journal.dynamodb

import akka.persistence.journal.JournalSpec
import akka.persistence.journal.dynamodb.DynamoDBJournal._
import com.amazonaws.services.dynamodbv2.model.{CreateTableRequest, DeleteTableRequest, ListTablesRequest, ProvisionedThroughput}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

class DynamoDBJournalSpec extends JournalSpec(ConfigFactory.load()) {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val config = system.settings.config.getConfig(Conf)
    val table = config.getString(JournalTable)
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
}

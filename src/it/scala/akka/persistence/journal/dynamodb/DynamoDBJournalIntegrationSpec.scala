package akka.persistence.journal.dynamodb

import akka.persistence.journal.JournalPerfSpec
import com.typesafe.config.ConfigFactory

class DynamoDBJournalIntegrationSpec extends JournalPerfSpec(ConfigFactory.load())
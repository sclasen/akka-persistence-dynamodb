package akka.persistence.journal.dynamodb

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model._

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

trait DynamoDBHelper {

  implicit val ec:ExecutionContext

  val dynamoDB:AmazonDynamoDBClient

  def toRight[T](t:T) = Right(t)

  def fold[T](fe: Future[Either[AmazonServiceException, T]]): Future[T] = fe.map(_.fold(e => throw e, t => t))

  def execute[T](f: => T)(implicit tag: ClassTag[T]) = Future(f).mapTo[T].map(toRight)

  def sendListTables(aws: ListTablesRequest): Future[ListTablesResult] = fold(listTables(aws))

  def listTables(aws: ListTablesRequest): Future[Either[AmazonServiceException, ListTablesResult]] =
    execute(dynamoDB.listTables(aws))

  def sendQuery(aws: QueryRequest): Future[QueryResult] = fold(query(aws))

  def query(aws: QueryRequest): Future[Either[AmazonServiceException, QueryResult]] =
    execute(dynamoDB.query(aws))

  def sendScan(aws: ScanRequest): Future[ScanResult] = fold(scan(aws))

  def scan(aws: ScanRequest): Future[Either[AmazonServiceException, ScanResult]] =
    execute(dynamoDB.scan(aws))

  def sendUpdateItem(aws: UpdateItemRequest): Future[UpdateItemResult] = fold(updateItem(aws))

  def updateItem(aws: UpdateItemRequest): Future[Either[AmazonServiceException, UpdateItemResult]] =
    execute(dynamoDB.updateItem(aws))

  def sendPutItem(aws: PutItemRequest): Future[PutItemResult] = fold(putItem(aws))

  def putItem(aws: PutItemRequest): Future[Either[AmazonServiceException, PutItemResult]] =
    execute(dynamoDB.putItem(aws))

  def sendDescribeTable(aws: DescribeTableRequest): Future[DescribeTableResult] = fold(describeTable(aws))

  def describeTable(aws: DescribeTableRequest): Future[Either[AmazonServiceException, DescribeTableResult]] =
    execute(dynamoDB.describeTable(aws))

  def sendCreateTable(aws: CreateTableRequest): Future[CreateTableResult] = fold(createTable(aws))

  def createTable(aws: CreateTableRequest): Future[Either[AmazonServiceException, CreateTableResult]] =
    execute(dynamoDB.createTable(aws))

  def sendUpdateTable(aws: UpdateTableRequest): Future[UpdateTableResult] = fold(updateTable(aws))

  def updateTable(aws: UpdateTableRequest): Future[Either[AmazonServiceException, UpdateTableResult]] =
    execute(dynamoDB.updateTable(aws))

  def sendDeleteTable(aws: DeleteTableRequest): Future[DeleteTableResult] = fold(deleteTable(aws))

  def deleteTable(aws: DeleteTableRequest): Future[Either[AmazonServiceException, DeleteTableResult]] =
    execute(dynamoDB.deleteTable(aws))

  def sendGetItem(aws: GetItemRequest): Future[GetItemResult] = fold(getItem(aws))

  def getItem(aws: GetItemRequest): Future[Either[AmazonServiceException, GetItemResult]] =
    execute(dynamoDB.getItem(aws))

  def sendBatchWriteItem(awsWrite: BatchWriteItemRequest): Future[BatchWriteItemResult] =
    fold(batchWriteItem(awsWrite))

  def batchWriteItem(aws: BatchWriteItemRequest): Future[Either[AmazonServiceException, BatchWriteItemResult]] =
    execute(dynamoDB.batchWriteItem(aws))

  def sendBatchGetItem(awsGet: BatchGetItemRequest): Future[BatchGetItemResult] = fold(batchGetItem(awsGet))

  def batchGetItem(aws: BatchGetItemRequest): Future[Either[AmazonServiceException, BatchGetItemResult]] =
    execute(dynamoDB.batchGetItem(aws))

  def sendDeleteItem(awsDel: DeleteItemRequest): Future[DeleteItemResult] = fold(deleteItem(awsDel))

  def deleteItem(aws: DeleteItemRequest): Future[Either[AmazonServiceException, DeleteItemResult]] =
    execute(dynamoDB.deleteItem(aws))

}

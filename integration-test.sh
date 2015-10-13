#!/bin/bash
echo "Compiling..."
sbt clean compile

DB_DIR="target/db"
PID_FILE="target/dynamo.pid"

if [ ! -f dynamodb-local/DynamoDBLocal.jar ]; then
    ./bin/get-dynamodb-local
fi

echo "Initializing db directory ${DB_DIR}"
mkdir ${DB_DIR}

echo "Starting up dynamod, pid is ${PID_FILE}"
rm -f ${PID_FILE}

nohup java -Djava.library.path=./dynamodb-local -jar ./dynamodb-local/DynamoDBLocal.jar -dbPath ${DB_DIR} > target/test-output.log 2>&1 &
echo $! > ${PID_FILE}

echo "Running load test"
sbt it:test

echo "Stopping dynamo"
kill -9 `cat ${PID_FILE}`




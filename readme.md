DynamoDBJournal for Akka Persistence
====================================

A replicated [Akka Persistence](http://doc.akka.io/docs/akka/2.3.0-RC3/scala/persistence.html) journal backed by
[Amazon DynamoDB](http://aws.amazon.com/dynamodb/).

Scala: `2.10.4` and `2.11.1`  Akka: `2.3.3`

Important Notice
----------------

***Further development of this plugin has moved to the Akka organization, please file issues and submit pull requests against [akka/akka-persistence-dynamodb](https://github.com/akka/akka-persistence-dynamodb).*** In particular you will find versions built for current versions of Scala and Akka at the new location, published under the `com.typesafe.akka` groupId.

Installation
------------

```scala

// build.sbt

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.sclasen" %% "akka-persistence-dynamodb" % "0.3.4" % "compile"

```

Configuration
-------------

```
// application.conf - all config except endpoint, journal-name, sequence-shards is required

akka.persistence.journal.plugin = "dynamodb-journal"

dynamodb-journal {
    journal-table =  "the name of a dynamodb table you create with hash key: `key`"
    # journal-name =  "prefixes all keys, allows multiple journals per table, default: `journal`"
    aws-access-key-id =  "yourKey"
    aws-secret-access-key =  "yourSecret"
    operation-timeout =  10 seconds
    # endpoint =  "defaults to https://dynamodb.us-east-1.amazonaws.com"
    sequence-shards = 1000
}

Note on `sequence-shards`: the high and low sequence numbers are stored across `sequence-shards` number of keys.
The more shards used, the less likely that throttling will occur when writing at a high rate.  Sequence shards should
be set to as least as high as the write throughput of your table.

The trade off with a higher number of shards is the number of requests needed to find the high or low sequence number for a processor.
We can read the value of 100 shards per request to dynamodb, so reading 1000 shards takes 10 (parallel) requests, 10000 takes 100, etc.

The reference.conf for this journal also contains the following settings for spray and akka.

```
spray.can.host-connector.max-connections = 600
akka.persistence.journal.max-message-batch-size = 4000
```

```

Development
-----------

### dev setup

* install [forego](https://github.com/ddollar/forego) if you dont have it.

* run `bin/get-dynamodb-local`

this downloads and unpacks the dynamodb local to a subdir of ./dynamodb-local

* `cp .env.sample .env`

* make sure the DYNAMODB_RELEASE var in .env matches the date of the distro that was placed in ./dynamodb-local

* `forego start`

This starts the local dynamodb instance

In another shell

* forego run sbt test

### dynamodb table structure discussion

the structure for journal storage in dynamodb has evolved over iterations of performance tuning. Most of these lessons were learned
in creating the eventsourced dynamodb journal, but apply here as well.

##### naiive structure

When initially modelling journal storage in dynamo, it seems natural to use a simple structure similar to this

```
processorId  : S : HashKey
sequenceNr   : N : RangeKey
deleted      : S
confirmations: SS
payload      : B
```

This maps very well to the operations a journal needs to solve.

```
writeMessage      -> PutItem
writeConfirmation -> UpdateItem with set add
deleteMessage     -> UpdateItem (mark deleted) or DeleteItem (permanent delete)
replayMessages    -> Query by processorId, conditions and ordered by sequenceNr, ascending
highCounter       -> Query by processorId, conditions and ordered by sequenceNr, descending limit 1
```

However this layout suffers from scalability problems. Since the hash key is used to locate the data storage node, all writes for a
single processor will go to the same dynamodb node, which limits throughput and invites throttling, no matter the level of throughput provisioned
for a table. The hash key just gets too hot. Also this limits replay throughput since you have to step through a sequence of queries, where
you use the last processed item in query N for query N+1.

##### higher throughput structure.

```
P -> PersistentRepr
SH -> SequenceHigh
SL -> SequenceLow

Persistent Data

journalName"-P"-processorId-sequenceNr  : S : HashKey
deleted                     : S
confirmations               : SS
payload                     : B

High and Low Sequence Numbers

journalName"-SH"-processorId-(sequenceNr % sequenceShards): S : HashKey
sequenceNr                                    : N

journalName"-SL"-processorId-(sequenceNr % sequenceShards): S : HashKey
sequenceNr                                    : N
```

This is somewhat more difficult to code, but offers higher throughput possibilities. Notice that the items that hold the high and low sequence are sharded,
rather than using a single item to store the counter. If we only used a single item, we would suffer from the same hot key problems as our
first structure.

```
writeMessage      -> BatchPutItem (P and SH)
writeConfirmation -> UpdateItem with set add
deleteMessage     -> BatchPutItem (mark deleted, set SL) or BatchPutItem (permanent delete, set SL)
replayMessages    -> Parallel BatchGet all P items for the processor
highCounter       -> Parallel BatchGet all SH shards for the processor, find max.
lowCounter        -> Parallel BatchGet all SL shards for the processor, find min.
```



# neo-utils-go

[![CircleCI](https://circleci.com/gh/Financial-Times/neo-utils-go.svg?style=svg)](https://circleci.com/gh/Financial-Times/neo-utils-go)

Neo4j Utils in Go.

Provides a wrapper for neoism.Database to output the database url in the String() method.

Provides and EnsureIndexes function that will take a map of label/property pairs,
and an IndexManager (normally this will be a neoism.Database) and checks whether those
indexes exist. If not, they are created.

## Batch Cypher Runner
Currently supports batch running of queries.

import "github.com/Financial-Times/neo-cypher-runner-go"

Create a batch cypher runner like this:

    cypherRunner := neocypherrunner.NewBatchCypherRunner(db, maxBatchSize)

Execute a batch of queries like this:

    cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

### Metrics
There are three metrics that this library will capture, using go-metrics:

 - batchQueueSize: a gauge which keeps track of the number of queued requests, i.e. waiting to be written to Neo4j
 - batchThroughput: a meter keeping track of queries processed, with throughput over several time periods
 - execute-neo4j-batch: a timer measuring how long each batch or queries takes to run against neo4j

To use the metrics, set up metrics in your application, for example to output to graphite.ft.com:

    addr, _ := net.ResolveTCPAddr("tcp", graphiteTCPAddress)
		go graphite.Graphite(metrics.DefaultRegistry, 1*time.Minute, graphitePrefix, addr)

where, for example:

 - graphiteTCPAddress is for graphite.ft.com:2003
 - and graphitePrefix is unique for your service, e.g. content.[env].people.rw.neo4j.[hostname] - you should probably set this from environment-specific configuration, e.g. hiera data

### Logging
This library uses go-logger v2 for logging. The logger is an optional parameter. If it is not provided by the user, the library creates a logger with an INFO logging level.

# neo-utils-go

[![CircleCI](https://circleci.com/gh/Financial-Times/neo-utils-go.svg?style=svg)](https://circleci.com/gh/Financial-Times/neo-utils-go)

Neo4j Utils in Go.

Provides a wrapper for neoism.Database to output the database URL in the String() method.

Provides an EnsureIndexes function that will take a map of label/property pairs,
and an IndexManager (normally this will be a neoism.Database) and checks whether these
indexes exist. If not, they are created.

## Batch Cypher Runner
Currently supports batch running of queries.

1. Import "github.com/Financial-Times/neo-cypher-runner-go"

2. Create a batch cypher runner like this:

    cypherRunner := neocypherrunner.NewBatchCypherRunner(db, maxBatchSize)

3. Execute a batch of queries like this:

    cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

### Logging
To use neo-utils-go in a service, follow these steps:
1. Migrate the service to Go modules and then to go-logger v2
2. Update the neo-utils-go version to v2.
3. Initialize the logger and add the instance to the function parameters in any of the neo-utils-go functions that requires it. 
The logger initialization will vary depending on the service logs.
// Non-JSON
l := logger.NewUnstructuredLogger()
// JSON, no "@time" key
l := logger.NewUPPLogger(*serviceName, "INFO")
// JSON + @time
logConf := logger.KeyNamesConfig{KeyTime: "@time"}
l := logger.NewUPPLogger(*serviceName, "INFO", logConf)
The logger is an optional parameter. If it is not provided by the user, the library will create a logger with an INFO logging level.

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

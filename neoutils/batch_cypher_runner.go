package neoutils

import (
	"github.com/rcrowley/go-metrics"
)

func NewBatchCypherRunner(cypherRunner CypherRunner, count int) CypherRunner {
	cr := BatchCypherRunner{cypherRunner, make(chan cypherQueryBatch, count), count}

	go cr.batcher()

	return &cr
}

type BatchCypherRunner struct {
	cr    CypherRunner
	ch    chan cypherQueryBatch
	count int
}

func (bcr *BatchCypherRunner) CypherBatch(queries []*CypherQuery) error {

	errCh := make(chan error)
	bcr.ch <- cypherQueryBatch{queries, errCh}
	return <-errCh
}

type cypherQueryBatch struct {
	queries []*CypherQuery
	err     chan error
}

func (bcr *BatchCypherRunner) batcher() {
	g := metrics.GetOrRegisterGauge("batchQueueSize", metrics.DefaultRegistry)
	b := metrics.GetOrRegisterMeter("batchThroughput", metrics.DefaultRegistry)
	for {
		var currentQueries []*CypherQuery
		var currentErrorChannels []chan error
		// wait for at least one
		cb := <-bcr.ch
		currentErrorChannels = append(currentErrorChannels, cb.err)
		for _, query := range cb.queries {
			currentQueries = append(currentQueries, query)
			g.Update(int64(len(currentQueries)))
		}
		// add any others pending (up to max size)
		for len(bcr.ch) > 0 && len(currentQueries) < bcr.count {
			cb = <-bcr.ch
			currentErrorChannels = append(currentErrorChannels, cb.err)
			for _, query := range cb.queries {
				currentQueries = append(currentQueries, query)
				g.Update(int64(len(currentQueries)))
			}

		}
		// run the batch of queries
		t := metrics.GetOrRegisterTimer("execute-neo4j-batch", metrics.DefaultRegistry)
		var err error
		t.Time(func() {
			err = processCypherBatch(bcr, currentQueries)
		})
		for _, cec := range currentErrorChannels {
			cec <- err
		}
		b.Mark(int64(len(currentQueries)))
		g.Update(0)
	}
}

func processCypherBatch(bcr *BatchCypherRunner, currentQueries []*CypherQuery) error {
	return bcr.cr.CypherBatch(currentQueries)
}

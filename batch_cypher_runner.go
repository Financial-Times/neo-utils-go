package neoutils

import (
	"fmt"
	"github.com/jmcvetta/neoism"
	"github.com/rcrowley/go-metrics"
	"log"
)

type CypherRunner interface {
	fmt.Stringer
	CypherBatch(queries []*neoism.CypherQuery) error
}

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

func (bcr *BatchCypherRunner) CypherBatch(queries []*neoism.CypherQuery) error {

	errCh := make(chan error)
	bcr.ch <- cypherQueryBatch{queries, errCh}
	return <-errCh
}

type cypherQueryBatch struct {
	queries []*neoism.CypherQuery
	err     chan error
}

func (bcr *BatchCypherRunner) batcher() {
	g := metrics.GetOrRegisterGauge("batchQueueSize", metrics.DefaultRegistry)
	b := metrics.GetOrRegisterMeter("batchThroughput", metrics.DefaultRegistry)
	var currentQueries []*neoism.CypherQuery
	var currentErrorChannels []chan error
	for {
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
			err = bcr.cr.CypherBatch(currentQueries)
		})
		if err != nil {
			log.Printf("ERROR Got error running batch, error=%v\n", err)
		}
		for _, cec := range currentErrorChannels {
			cec <- err
		}
		b.Mark(int64(len(currentQueries)))
		g.Update(0)
		currentQueries = currentQueries[0:0] // clears the slice
		currentErrorChannels = currentErrorChannels[0:0]
	}
}

func (bcr *BatchCypherRunner) String() string {
	return fmt.Sprintf("BatchCypherRunner(%s)", bcr.cr.String())
}

package neoutils

import (
	"encoding/json"
	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
	"github.com/jmcvetta/neoism"
	"github.com/rcrowley/go-metrics"
	"log"
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
	for {
		var currentQueries []*neoism.CypherQuery
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

func processCypherBatch(bcr *BatchCypherRunner, currentQueries []*neoism.CypherQuery) error {
	err := bcr.cr.CypherBatch(currentQueries)
	if err != nil {
		if neoErr, ok := err.(neoism.NeoError); ok && neoErr.Exception == "BatchOperationFailedException" {
			neoErrMsg := struct {
				Message string           `json:"message"`
				Errors  []neoism.TxError `json:"errors"`
			}{}

			if jsonErr := json.Unmarshal([]byte(neoErr.Message), &neoErrMsg); jsonErr != nil {
				log.Printf("ERROR Got error trying to process Neo Error Message, error=%v\n", jsonErr)
				return err
			}

			for _, nerr := range neoErrMsg.Errors {
				if nerr.Code == "Neo.ClientError.Schema.ConstraintViolation" {
					return rwapi.ConstraintOrTransactionError{Message: nerr.Message}
				}
			}
		}
	}
	return err
}

package neoutils

import (
	"errors"
	"testing"
	"time"

	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
)

func TestAllQueriesRun(t *testing.T) {
	assert := assert.New(t)
	mr := &mockRunner{}
	batchCypherRunner := NewBatchCypherRunner(mr, 3)

	errCh := make(chan error)

	go func() {
		errCh <- batchCypherRunner.CypherBatch([]*neoism.CypherQuery{
			&neoism.CypherQuery{Statement: "First"},
			&neoism.CypherQuery{Statement: "Second"},
		})
	}()

	go func() {
		time.Sleep(time.Millisecond * 1)
		errCh <- batchCypherRunner.CypherBatch([]*neoism.CypherQuery{
			&neoism.CypherQuery{Statement: "Third"},
		})
	}()

	for i := 0; i < 2; i++ {
		err := <-errCh
		assert.NoError(err, "Got an error for %d", i)
	}

	expected := []*neoism.CypherQuery{
		&neoism.CypherQuery{Statement: "First"},
		&neoism.CypherQuery{Statement: "Second"},
		&neoism.CypherQuery{Statement: "Third"},
	}

	assert.Equal(expected, mr.queriesRun, "queries didn't match")
}

func TestQueryBatching(t *testing.T) {
	assert := assert.New(t)

	dr := &delayRunner{make(chan []*neoism.CypherQuery)}
	batchCypherRunner := NewBatchCypherRunner(dr, 3)

	errCh := make(chan error)

	go func() {
		errCh <- batchCypherRunner.CypherBatch([]*neoism.CypherQuery{
			&neoism.CypherQuery{Statement: "First"},
		})
	}()

	go func() {
		time.Sleep(time.Millisecond * 1)
		errCh <- batchCypherRunner.CypherBatch([]*neoism.CypherQuery{
			&neoism.CypherQuery{Statement: "Second"},
		})
	}()

	go func() {
		time.Sleep(time.Millisecond * 2)
		errCh <- batchCypherRunner.CypherBatch([]*neoism.CypherQuery{
			&neoism.CypherQuery{Statement: "Third"},
		})
	}()

	time.Sleep(3 * time.Millisecond)

	// Only "First" can have finished because delayRunner is blocking the others until we read from its channel.
	assert.Equal([]*neoism.CypherQuery{
		&neoism.CypherQuery{Statement: "First"},
	}, <-dr.queriesRun)

	// because of the time.Sleep() calls earlier, these should both be ready by now.
	assert.Equal([]*neoism.CypherQuery{
		&neoism.CypherQuery{Statement: "Second"},
		&neoism.CypherQuery{Statement: "Third"},
	}, <-dr.queriesRun)

	for i := 0; i < 3; i++ {
		err := <-errCh
		assert.NoError(err, "Got an error for %d", i)
	}

}

func TestEveryoneGetsErrorOnFailure(t *testing.T) {
	assert := assert.New(t)
	mr := &failRunner{}
	batchCypherRunner := NewBatchCypherRunner(mr, 3)

	errCh := make(chan error)

	go func() {
		errCh <- batchCypherRunner.CypherBatch([]*neoism.CypherQuery{
			&neoism.CypherQuery{Statement: "First"},
			&neoism.CypherQuery{Statement: "Second"},
		})
	}()

	go func() {
		errCh <- batchCypherRunner.CypherBatch([]*neoism.CypherQuery{
			&neoism.CypherQuery{Statement: "Third"},
		})
	}()

	for i := 0; i < 2; i++ {
		err := <-errCh
		assert.Error(err, "Didn't get an error for %d", i)
	}

	assert.Equal(len(errCh), 0, "too many errors")
}

type mockRunner struct {
	queriesRun []*neoism.CypherQuery
}

func (mr *mockRunner) CypherBatch(queries []*neoism.CypherQuery) error {
	mr.queriesRun = append(mr.queriesRun, queries...)
	return nil
}

func (mr *mockRunner) String() string {
	return "URL"
}

type failRunner struct {
}

func (mr *failRunner) CypherBatch(queries []*neoism.CypherQuery) error {
	return errors.New("UNIT TESTING: Deliberate fail for every query")
}

func (mr *failRunner) String() string {
	return "URL"
}

type delayRunner struct {
	queriesRun chan []*neoism.CypherQuery
}

func (dr *delayRunner) CypherBatch(queries []*neoism.CypherQuery) error {
	dr.queriesRun <- queries
	return nil
}

func (dr *delayRunner) String() string {
	return "URL"
}

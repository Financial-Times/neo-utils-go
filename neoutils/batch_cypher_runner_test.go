package neoutils

import (
	"errors"
	"testing"
	"time"

	"fmt"

	"github.com/stretchr/testify/assert"
)

func TestAllQueriesRun(t *testing.T) {
	mr := &mockRunner{}
	batchCypherRunner := NewBatchCypherRunner(mr, 3)

	errCh := make(chan error)

	go func() {
		errCh <- batchCypherRunner.CypherBatch([]*CypherQuery{
			{Statement: "First"},
			{Statement: "Second"},
		})
	}()

	go func() {
		time.Sleep(time.Millisecond * 1)
		errCh <- batchCypherRunner.CypherBatch([]*CypherQuery{
			{Statement: "Third"},
		})
	}()

	for i := 0; i < 2; i++ {
		err := <-errCh
		assert.NoError(t, err, "Got an error for %d", i)
	}

	expected := []*CypherQuery{
		{Statement: "First"},
		{Statement: "Second"},
		{Statement: "Third"},
	}

	assert.Equal(t, expected, mr.queriesRun, "queries didn't match")
}

func TestQueryBatching(t *testing.T) {
	dr := &delayRunner{make(chan []*CypherQuery)}
	batchCypherRunner := NewBatchCypherRunner(dr, 3)

	errCh := make(chan error)

	go func() {
		errCh <- batchCypherRunner.CypherBatch([]*CypherQuery{
			{Statement: "First"},
		})
	}()

	go func() {
		time.Sleep(time.Millisecond * 10)
		errCh <- batchCypherRunner.CypherBatch([]*CypherQuery{
			{Statement: "Second"},
		})
	}()

	go func() {
		time.Sleep(time.Millisecond * 20)
		errCh <- batchCypherRunner.CypherBatch([]*CypherQuery{
			{Statement: "Third"},
		})
	}()

	time.Sleep(30 * time.Millisecond)

	// Only "First" can have finished because delayRunner is blocking the others until we read from its channel.
	assert.Equal(t, []*CypherQuery{
		{Statement: "First"},
	}, <-dr.queriesRun)

	// because of the time.Sleep() calls earlier, these should both be ready by now.
	assert.Equal(t, []*CypherQuery{
		{Statement: "Second"},
		{Statement: "Third"},
	}, <-dr.queriesRun)

	for i := 0; i < 3; i++ {
		err := <-errCh
		assert.NoError(t, err, "Got an error for %d", i)
	}

}

func TestEveryoneGetsErrorOnFailure(t *testing.T) {
	mr := &failRunner{}
	batchCypherRunner := NewBatchCypherRunner(mr, 3)

	errCh := make(chan error)

	go func() {
		errCh <- batchCypherRunner.CypherBatch([]*CypherQuery{
			{Statement: "First"},
			{Statement: "Second"},
		})
	}()

	go func() {
		errCh <- batchCypherRunner.CypherBatch([]*CypherQuery{
			{Statement: "Third"},
		})
	}()

	for i := 0; i < 2; i++ {
		err := <-errCh
		assert.Error(t, err, "Didn't get an error for %d", i)
	}

	assert.Equal(t, len(errCh), 0, "too many errors")
}

func TestAttemptToWriteConflictItem(t *testing.T) {
	db := connectTest(t)
	mr := StringerDb{db}
	batchCypherRunner := NewBatchCypherRunner(mr, 3)
	errCh := make(chan error)

	defer cleanup(t, db)
	defer cleanupConstraints(t, db)

	var res int
	go func() {
		fmt.Println("Batching...")
		errCh <- batchCypherRunner.CypherBatch([]*CypherQuery{
			{Statement: "CREATE (x:NeoUtilsTest { name : 'Andres', title : 'Developer' })"},
			{Statement: "CREATE (x:NeoUtilsTest { name : 'Bob', title : 'Builder' })"},
			{
				Statement: "MATCH (x:NeoUtilsTest) return count(x) as rs",
				Result:    &res,
			},
		})
		fmt.Println("Done...")
	}()

	assert.Equal(t, len(errCh), 0, "too many errors")
	var err = <-errCh
	assert.NoError(t, err)
	assert.NotEmpty(t, res)
	assert.Equal(t, 2, res)

	go func() {
		errCh <- batchCypherRunner.CypherBatch([]*CypherQuery{
			{Statement: "CREATE (x:NeoUtilsTest { name : 'Andres', title : 'Should fail' })"},
		})
	}()
	err = <-errCh
	assert.Error(t, err)
	assert.IsType(t, NeoErrors{}, err)
}

type mockRunner struct {
	queriesRun []*CypherQuery
}

func (mr *mockRunner) CypherBatch(queries []*CypherQuery) error {
	mr.queriesRun = append(mr.queriesRun, queries...)
	return nil
}

func (mr *mockRunner) String() string {
	return "URL"
}

type failRunner struct {
}

func (mr *failRunner) CypherBatch(queries []*CypherQuery) error {
	return errors.New("UNIT TESTING: Deliberate fail for every query")
}

func (mr *failRunner) String() string {
	return "URL"
}

type delayRunner struct {
	queriesRun chan []*CypherQuery
}

func (dr *delayRunner) CypherBatch(queries []*CypherQuery) error {
	dr.queriesRun <- queries
	return nil
}

func (dr *delayRunner) String() string {
	return "URL"
}

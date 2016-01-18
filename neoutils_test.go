package neoutils

import (
	"errors"
	"testing"

	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
)

func TestIndexesGetCreatedIfMissing(t *testing.T) {
	assert := assert.New(t)
	mIM := mockIndexManager{}
	indexes := map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid"}

	err := EnsureIndexes(mIM, indexes)
	assert.NoError(err, "Unexpected error")
}

func TestIndexesAreNotRecreatedIfPresent(t *testing.T) {
	assert := assert.New(t)
	indexes := map[string]string{
		"Thing": "uuid"}

	existingIndexes := []*neoism.Index{&neoism.Index{PropertyKeys: []string{"uuid"}}}

	mIM := mockIndexManager{existingIndexes}

	err := EnsureIndexes(mIM, indexes)
	assert.NoError(err, "Unexpected error")
}

func TestCheckSucceedsIfCanConnectToNeo4j(t *testing.T) {
	assert := assert.New(t)
	mCR := mockCypherRunner{}
	err := Check(mCR)
	assert.NoError(err, "Unexpected error")

}

func TestCheckErrorsIfCannotConnectToNeo4j(t *testing.T) {
	assert := assert.New(t)
	mCR := mockCypherRunner{true}
	err := Check(mCR)
	assert.Error(err, "Didn't get expected error")
}

type mockIndexManager struct {
	existingIndexes []*neoism.Index
}

func (mIM mockIndexManager) CreateIndex(label string, propertyName string) (*neoism.Index, error) {

	if len(mIM.existingIndexes) > 0 {
		return nil, errors.New("Shouldn't call CreateIndex if there are existing indexes already")
	}
	return &neoism.Index{}, nil
}

func (mIM mockIndexManager) Indexes(label string) ([]*neoism.Index, error) {
	return mIM.existingIndexes, nil
}

type mockCypherRunner struct {
	fail bool
}

func (mCR mockCypherRunner) CypherBatch(queries []*neoism.CypherQuery) error {
	if mCR.fail == true {
		return errors.New("Fail to run query")
	}
	return nil
}

func (mCR mockCypherRunner) String() string {
	return "URL"
}

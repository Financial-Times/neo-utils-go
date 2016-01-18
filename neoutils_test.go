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

	mIM := mockIndexManager{existingIndexes: existingIndexes}

	err := EnsureIndexes(mIM, indexes)
	assert.NoError(err, "Unexpected error")
}

func TestConstraintsAreCreatedIfMissing(t *testing.T) {
	assert := assert.New(t)
	mIM := mockIndexManager{}
	constraints := map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid"}

	err := EnsureConstraints(mIM, constraints)
	assert.NoError(err, "Unexpected error")
}

func TestConstraintsAreNotRecreatedIfPresent(t *testing.T) {
	assert := assert.New(t)
	constraints := map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid"}

	existingConstraints := []*neoism.UniqueConstraint{&neoism.UniqueConstraint{PropertyKeys: []string{"uuid"}}}

	mIM := mockIndexManager{existingConstraints: existingConstraints}

	err := EnsureConstraints(mIM, constraints)
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
	existingIndexes     []*neoism.Index
	existingConstraints []*neoism.UniqueConstraint
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

func (mIM mockIndexManager) CreateUniqueConstraint(label string, propertyName string) (*neoism.UniqueConstraint, error) {

	if len(mIM.existingConstraints) > 0 {
		return nil, errors.New("Shouldn't call CreateUniqueConstraints if there are existing constraints already")
	}
	return &neoism.UniqueConstraint{}, nil
}

func (mIM mockIndexManager) UniqueConstraints(label string, propertyName string) ([]*neoism.UniqueConstraint, error) {
	return mIM.existingConstraints, nil
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

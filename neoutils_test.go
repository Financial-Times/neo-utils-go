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

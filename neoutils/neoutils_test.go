package neoutils

import (
	"errors"
	"github.com/Financial-Times/go-logger"
	"testing"

	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
	"os"
)

func init() {
	logger.InitLogger("test-neo4j-utils-go", "warn")
}

func TestIndexesGetCreatedIfMissing(t *testing.T) {
	mIM := mockIndexManager{}
	indexes := map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid"}

	err := EnsureIndexes(mIM, indexes)
	assert.NoError(t, err, "Unexpected error")
}

func TestIndexesAreNotRecreatedIfPresent(t *testing.T) {
	indexes := map[string]string{
		"Thing": "uuid"}

	existingIndexes := []*neoism.Index{{PropertyKeys: []string{"uuid"}}}

	mIM := mockIndexManager{existingIndexes: existingIndexes}

	err := EnsureIndexes(mIM, indexes)
	assert.NoError(t, err, "Unexpected error")
}

func TestConstraintsAreCreatedIfMissing(t *testing.T) {
	mIM := mockIndexManager{}
	constraints := map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid"}

	err := EnsureConstraints(mIM, constraints)
	assert.NoError(t, err, "Unexpected error")
}

func TestConstraintsAreNotRecreatedIfPresent(t *testing.T) {
	constraints := map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid"}

	existingConstraints := []*neoism.UniqueConstraint{{PropertyKeys: []string{"uuid"}}}

	mIM := mockIndexManager{existingConstraints: existingConstraints}

	err := EnsureConstraints(mIM, constraints)
	assert.NoError(t, err, "Unexpected error")
}

func TestCheckSucceedsIfCanConnectToNeo4j(t *testing.T) {
	mCR := mockCypherRunner{}
	err := Check(mCR)
	assert.NoError(t, err, "Unexpected error")

}

func TestCheckErrorsIfCannotConnectToNeo4j(t *testing.T) {
	mCR := mockCypherRunner{true}
	err := Check(mCR)
	assert.Error(t, err, "Didn't get expected error")
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
	if len(mIM.existingIndexes) == 0 {
		return nil, neoism.NotFound
	}
	return mIM.existingIndexes, nil
}

func (mIM mockIndexManager) CreateUniqueConstraint(label string, propertyName string) (*neoism.UniqueConstraint, error) {

	if len(mIM.existingConstraints) > 0 {
		return nil, errors.New("Shouldn't call CreateUniqueConstraints if there are existing constraints already")
	}
	return &neoism.UniqueConstraint{}, nil
}

func (mIM mockIndexManager) UniqueConstraints(label string, propertyName string) ([]*neoism.UniqueConstraint, error) {
	if len(mIM.existingConstraints) == 0 {
		return nil, neoism.NotFound
	}
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

func connectTest(t *testing.T) *neoism.Database {
	neo4jURL := os.Getenv("NEO4J_URL")
	if neo4jURL == "" {
		neo4jURL = "http://localhost:7474/db/data"
	}

	db, err := neoism.Connect(neo4jURL)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.CreateUniqueConstraint("NeoUtilsTest", "name"); err != nil {
		t.Fatal(err)
	}
	return db
}

func cleanup(t *testing.T, db *neoism.Database) {

	err := db.CypherBatch([]*neoism.CypherQuery{
		{
			Statement: `MATCH (x:NeoUtilsTest) DETACH DELETE x`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func cleanupConstraints(t *testing.T, db *neoism.Database) {
	err := db.CypherBatch([]*neoism.CypherQuery{
		{
			Statement: `DROP CONSTRAINT ON (x:NeoUtilsTest) ASSERT x.name IS UNIQUE`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

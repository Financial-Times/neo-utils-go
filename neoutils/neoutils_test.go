package neoutils

import (
	"errors"
	"testing"

	"os"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/stretchr/testify/assert"
)

func TestIndexesGetCreatedIfMissing(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	mIM := mockIndexManager{}
	indexes := map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid"}

	err := EnsureIndexes(mIM, indexes, l)
	assert.NoError(t, err, "Unexpected error")
}

func TestIndexesAreNotRecreatedIfPresent(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	indexes := map[string]string{
		"Thing": "uuid"}

	existingIndexes := []Index{{Properties: []string{"uuid"}}}

	mIM := mockIndexManager{existingIndexes: existingIndexes}

	err := EnsureIndexes(mIM, indexes, l)
	assert.NoError(t, err, "Unexpected error")
}

func TestConstraintsAreCreatedIfMissing(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	mIM := mockIndexManager{}
	constraints := map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid"}

	err := EnsureConstraints(mIM, constraints, l)
	assert.NoError(t, err, "Unexpected error")
}

func TestConstraintsAreNotRecreatedIfPresent(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	constraints := map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid"}

	existingConstraints := []UniqueConstraint{{Properties: []string{"uuid"}}}

	mIM := mockIndexManager{existingConstraints: existingConstraints}

	err := EnsureConstraints(mIM, constraints, l)
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
	existingIndexes     []Index
	existingConstraints []UniqueConstraint
}

func (mIM mockIndexManager) CreateIndex(label string, propertyName string) error {

	if len(mIM.existingIndexes) > 0 {
		return errors.New("Shouldn't call CreateIndex if there are existing indexes already")
	}
	return nil
}

func (mIM mockIndexManager) Indexes(label string) ([]Index, error) {
	if len(mIM.existingIndexes) == 0 {
		return nil, nil
	}
	return mIM.existingIndexes, nil
}

func (mIM mockIndexManager) CreateUniqueConstraint(label string, propertyName string) error {

	if len(mIM.existingConstraints) > 0 {
		return errors.New("Shouldn't call CreateUniqueConstraints if there are existing constraints already")
	}
	return nil
}

func (mIM mockIndexManager) UniqueConstraints(label string, propertyName string) ([]UniqueConstraint, error) {
	if len(mIM.existingConstraints) == 0 {
		return nil, nil
	}
	return mIM.existingConstraints, nil
}

type mockCypherRunner struct {
	fail bool
}

func (mCR mockCypherRunner) CypherBatch(queries []*CypherQuery) error {
	if mCR.fail == true {
		return errors.New("Fail to run query")
	}
	return nil
}

func (mCR mockCypherRunner) String() string {
	return "URL"
}

func connectTest(t *testing.T) Database {
	neo4jURL := os.Getenv("NEO4J_TEST_URL")
	if neo4jURL == "" {
		neo4jURL = "http://localhost:7474/db/neo4j"
	}

	db, err := NewDatabase(neo4jURL, DefaultConnectionConfig())
	if err != nil {
		t.Fatal(err)
	}

	if err := db.CreateUniqueConstraint("NeoUtilsTest", "name"); err != nil {
		t.Fatal(err)
	}
	return db
}

func cleanup(t *testing.T, db Database) {

	err := db.CypherBatch([]*CypherQuery{
		{
			Statement: `MATCH (x:NeoUtilsTest) DETACH DELETE x`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func cleanupConstraints(t *testing.T, db Database) {
	err := db.CypherBatch([]*CypherQuery{
		{
			Statement: `DROP CONSTRAINT ON (x:NeoUtilsTest) ASSERT x.name IS UNIQUE`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

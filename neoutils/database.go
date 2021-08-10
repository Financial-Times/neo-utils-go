package neoutils

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

// Database
type Database interface {
	// CreateIndex starts a background job in the database that will create and
	// populate the new index of a specified property on nodes of a given label.
	CreateIndex(label, propertyName string) error

	// Indexes lists indexes for a label.  If a blank string is given as the label,
	// returns all indexes.
	Indexes(label string) ([]Index, error)

	// UniqueConstraints get a specific unique constraint for a label and a property.
	// If a blank string is given as the property, return all unique constraints for
	// the label.
	UniqueConstraints(label, propertyName string) ([]UniqueConstraint, error)

	// CreateUniqueConstraint create a unique constraint on a property on nodes
	// with a specific label.
	CreateUniqueConstraint(label, propertyName string) error

	CypherBatch(queries []*CypherQuery) error

	String() string
}

type neoDB struct {
	session *Session
	url     string
}

func (db *neoDB) String() string {
	return db.url
}

func (db *neoDB) CreateIndex(label, propertyName string) error {
	// Neo4j doesn't work if the statement is passed with parameters
	statement := fmt.Sprintf(`CREATE INDEX %s FOR (n:%s) ON (n.%s)`,
		label+"_"+propertyName, label, propertyName)
	query := &CypherQuery{
		Statement: statement,
	}
	err := db.session.Send(query)
	return err
}

func (db *neoDB) Indexes(label string) ([]Index, error) {
	var statement string
	if label == "" {
		statement = "SHOW INDEXES"
	} else {
		statement = "SHOW INDEXES WHERE $label IN labelsOrTypes"
	}
	var indexes []struct {
		Name   string   `json:"name"`
		Labels []string `json:"labelsOrTypes"`
		Props  []string `json:"properties"`
	}
	query := &CypherQuery{
		Statement: statement,
		Parameters: map[string]interface{}{
			"label": label,
		},
		Result: &indexes,
	}
	err := db.session.Send(query)
	if err != nil {
		return nil, err
	}
	result := make([]Index, 0, len(indexes))
	for _, i := range indexes {
		idx := Index{Name: i.Name, Properties: i.Props}
		if len(i.Labels) > 0 {
			idx.Label = i.Labels[len(i.Labels)-1]
		}
		result = append(result, idx)
	}
	return result, nil
}

func (db *neoDB) UniqueConstraints(label, propertyName string) ([]UniqueConstraint, error) {
	if label == "" {
		return nil, errors.New("labels not provided")
	}
	var statement string
	if propertyName == "" {
		statement = "SHOW UNIQUE CONSTRAINTS WHERE $label IN labelsOrTypes"
	} else {
		statement = `
SHOW UNIQUE CONSTRAINTS 
WHERE $label IN labelsOrTypes 
    AND $property IN properties`
	}
	var ucs []struct {
		Name   string   `json:"name"`
		Labels []string `json:"labelsOrTypes"`
		Props  []string `json:"properties"`
	}
	query := &CypherQuery{
		Statement: statement,
		Parameters: map[string]interface{}{
			"label":    label,
			"property": propertyName,
		},
		Result: &ucs,
	}
	err := db.session.Send(query)
	if err != nil {
		return nil, err
	}
	result := make([]UniqueConstraint, 0, len(ucs))
	for _, uc := range ucs {
		idx := UniqueConstraint{Name: uc.Name, Properties: uc.Props}
		if len(uc.Labels) > 0 {
			idx.Label = uc.Labels[len(uc.Labels)-1]
		}
		result = append(result, idx)
	}
	return result, nil
}

func (db *neoDB) CreateUniqueConstraint(label, propertyName string) error {
	if label == "" || propertyName == "" {
		return errors.New("label or propertyName not set")
	}
	// Neo4j doesn't work if the statement is passed with parameters
	statement := fmt.Sprintf(`
CREATE CONSTRAINT %s IF NOT EXISTS
ON (m:%s)
ASSERT m.%s IS UNIQUE`, label+"_"+propertyName, label, propertyName)
	query := &CypherQuery{
		Statement: statement,
	}
	err := db.session.Send(query)
	return err
}

func (db *neoDB) CypherBatch(queries []*CypherQuery) error {
	return db.session.Send(queries...)
}

// TODO: Use the discovery endpoint for validation
func NewDatabase(url string, config *ConnectionConfig) (Database, error) {
	h := http.Header{}
	exeName, err := os.Executable()
	if err != nil {
		return nil, err
	}
	_, exeFile := filepath.Split(exeName)
	h.Set("User-Agent", exeFile+" (using neoutils)")
	h.Set("Content-Type", "application/json")

	db := &neoDB{
		url: url,
		session: &Session{
			Client:           config.HTTPClient,
			Header:           &h,
			CommitTxEndpoint: url + "/tx/commit",
		},
	}

	return db, nil
}

// Cypher
type CypherQuery struct {
	// used by Neo4j HTTP API
	Statement  string                 `json:"statement"`
	Parameters map[string]interface{} `json:"parameters"`

	// Used by neoism, for compatibility purposes
	Result interface{} `json:"-"`
}

// Indexes
type Index struct {
	Name       string
	Label      string
	Properties []string
}

// Constraints
type UniqueConstraint struct {
	Name       string
	Label      string
	Type       string
	Properties []string
}

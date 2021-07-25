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
	query := &CypherQuery{
		Statement: `CREATE INDEX $indexName FOR (n:$label) ON (n.$propertyName)`,
		Parameters: map[string]interface{}{
			"indexName":    label + "_" + propertyName,
			"label":        label,
			"propertyName": propertyName,
		},
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
	// The response is heterogeneous list and must be parsed mannualy
	var indexes [][]interface{}
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
	var result []Index
	for _, idxSlice := range indexes {
		if len(idxSlice) != 10 {
			msg := "SHOW INDEXES response is not in the expected format"
			return nil, errors.New(msg)
		}
		// Some system indexes doesn't have labels and props
		if idxSlice[7] == nil || idxSlice[8] == nil {
			continue
		}
		idxName, ok := idxSlice[1].(string)
		if !ok {
			return nil, errors.New("couldn't get index name")
		}
		labels, ok := idxSlice[7].([]interface{})
		if !ok {
			return nil, errors.New("couldn't get index labels")
		}
		label, ok := labels[len(labels)-1].(string)
		if !ok {
			return nil, errors.New("couldn't get index last label")
		}
		propsInterface, ok := idxSlice[8].([]interface{})
		props := make([]string, 0, len(propsInterface))
		if !ok {
			return nil, errors.New("couldn't get index properties")
		} else {
			for _, prop := range propsInterface {
				s, ok := prop.(string)
				if !ok {
					msg := "index propertyName is not of type string"
					return nil, errors.New(msg)
				}
				props = append(props, s)
			}
		}
		result = append(result, Index{
			Name:       idxName,
			Label:      label,
			Properties: props,
		})
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
	// The response is heterogeneous list and must be parsed mannualy
	var constraints []interface{}
	query := &CypherQuery{
		Statement: statement,
		Parameters: map[string]interface{}{
			"label":    label,
			"property": propertyName,
		},
		Result: &constraints,
	}
	err := db.session.Send(query)
	if err != nil {
		return nil, err
	}
	if len(constraints) == 0 {
		return nil, nil
	}
	parseConstraintRow := func(v []interface{}) (uc UniqueConstraint, err error) {
		name, ok := v[1].(string)
		if !ok {
			return uc, errors.New("couldn't get constraint name")
		}
		labels, ok := v[4].([]interface{})
		if !ok {
			return uc, errors.New("couldn't get constraint labels")
		}
		label, ok := labels[len(labels)-1].(string)
		if !ok {
			return uc, errors.New("couldn't get constraint last label")
		}
		propsInterface, ok := v[5].([]interface{})
		props := make([]string, 0, len(propsInterface))
		if !ok {
			return uc, errors.New("couldn't get constraint properties")
		} else {
			for _, prop := range propsInterface {
				s, ok := prop.(string)
				if !ok {
					msg := "index propertyName is not of type string"
					return uc, errors.New(msg)
				}
				props = append(props, s)
			}
		}
		uc = UniqueConstraint{Name: name, Label: label, Properties: props}
		return uc, nil
	}
	// If the first element is a slice than the result is [][]interface{}
	_, ok := constraints[0].([]interface{})
	var result []UniqueConstraint
	if ok {
		for _, c := range constraints {
			c, ok := c.([]interface{})
			if !ok {
				return nil, errors.New("constraint rows must be of the same type")
			}
			r, err := parseConstraintRow(c)
			if err != nil {
				return nil, err
			}
			result = append(result, r)
		}
	} else {
		r, err := parseConstraintRow(constraints)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
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
// TODO: Return interface
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

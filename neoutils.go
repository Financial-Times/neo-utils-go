package neoutils

import (
	"github.com/Financial-Times/neo-cypher-runner-go"
	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
)

// StringerDb wraps neoism Database to provide a String function, which outputs the database URL
type StringerDb struct{ *neoism.Database }

func (sdb StringerDb) String() string {
	return sdb.Url
}

// Check will use the supplied CypherRunner to check connectivity to Neo4j
func Check(cr neocypherrunner.CypherRunner) error {
	results := []struct {
		node interface{}
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n) RETURN id(n) LIMIT 1`,
		Result:    &results,
	}

	err := cr.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return err
	}
	return nil
}

// EnsureIndexes will, for a map of labels and properties, check whether an index exists for a given property on a given label, and if missing will create one
func EnsureIndexes(im IndexManager, indexes map[string]string) error {
	for label, propertyName := range indexes {
		err := ensureIndex(im, label, propertyName)
		if err != nil { // stop as soon as something goes wrong
			return err
		}
	}
	return nil
}

// EnsureConstraints will, for a map of labels and properties, check whether a constraint exists for a given property on a given label, and
// if missing will create one. Creating the unique constraint ensures an index automatically.
func EnsureConstraints(im IndexManager, indexes map[string]string) error {
	for label, propertyName := range indexes {
		err := ensureConstraint(im, label, propertyName)
		if err != nil { // stop as soon as something goes wrong
			return err
		}
	}
	return nil
}

func ensureIndex(im IndexManager, label string, propertyName string) error {

	indexes, err := im.Indexes(label)

	if err != nil {
		return err
	}

	var indexFound bool

	for _, index := range indexes {
		if len(index.PropertyKeys) == 1 && index.PropertyKeys[0] == propertyName {
			indexFound = true
			break
		}
	}

	if !indexFound {
		log.Infof("Creating index for type %s on property %s\n", label, propertyName)
		_, err := im.CreateIndex(label, propertyName)
		if err != nil {
			return err
		}
	}
	return nil

}

func ensureConstraint(im IndexManager, label string, propertyName string) error {

	constraints, err := im.UniqueConstraints(label, propertyName)

	if err != nil {
		return err
	}

	var constraintFound bool

	for _, constraint := range constraints {
		if len(constraint.PropertyKeys) == 1 && constraint.PropertyKeys[0] == propertyName {
			constraintFound = true
			break
		}
	}
	if !constraintFound {
		log.Infof("Creating unique constraint for type %s on property %s\n", label, propertyName)
		_, err = im.CreateUniqueConstraint(label, propertyName)
		if err != nil {
			return err
		}
	}
	return nil

}

func getIndex(im IndexManager, label string, propertyName string) (*neoism.Index, error) {
	indexes, err := im.Indexes(label)

	if err != nil {
		return nil, err
	}

	for _, index := range indexes {
		if len(index.PropertyKeys) == 1 && index.PropertyKeys[0] == propertyName {
			return index, nil
		}
	}
	return nil, nil
}

// IndexManager manages the maintenance of indexes and unique constraints
type IndexManager interface {
	CreateIndex(label string, propertyName string) (*neoism.Index, error)
	Indexes(label string) ([]*neoism.Index, error)
	CreateUniqueConstraint(label string, propertyName string) (*neoism.UniqueConstraint, error)
	UniqueConstraints(label string, propertyName string) ([]*neoism.UniqueConstraint, error)
}

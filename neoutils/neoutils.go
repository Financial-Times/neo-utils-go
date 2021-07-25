package neoutils

import (
	"errors"
	"fmt"

	"github.com/Financial-Times/go-logger/v2"
)

//  StringerDb wraps neoism Database to provide a String function, which outputs the database URL
type StringerDb struct{ Database }

// Check will use the supplied CypherRunner to check connectivity to Neo4j
func Check(cr CypherRunner) error {
	var results []struct {
		node interface{}
	}

	query := &CypherQuery{
		Statement: `MATCH (n) RETURN id(n) LIMIT 1`,
		Result:    &results,
	}

	err := cr.CypherBatch([]*CypherQuery{query})

	if err != nil {
		return err
	}
	return nil
}

// CheckWritable calls the dbms.cluster.role() procedure and verifies the role if it's LEADER or not.
func CheckWritable(cr CypherRunner) error {

	var res []struct {
		Role string `json:"role"`
	}

	query := &CypherQuery{
		Statement: `CALL dbms.cluster.role("neo4j")`,
		Result:    &res,
	}

	err := cr.CypherBatch([]*CypherQuery{query})

	if err != nil {
		return err
	}

	if len(res) == 0 || res[0].Role == "" {
		return errors.New("got empty response from dbms.cluster.role()")
	}

	role := res[0].Role

	if role != "LEADER" {
		return errors.New("role has to be LEADER for writing but it's " + role)
	}

	return nil
}

// EnsureIndexes will, for a map of labels and properties, check whether an index exists for a given property on a given label, and if missing will create one.
func EnsureIndexes(im IndexManager, indexes map[string]string, log *logger.UPPLogger) error {
	// log is an optional parameter
	if log == nil {
		log = logger.NewUPPInfoLogger("neo-utils-go")
	}
	for label, propertyName := range indexes {
		err := ensureIndex(im, label, propertyName, log)
		if err != nil { // stop as soon as something goes wrong
			return err
		}
	}
	return nil
}

// EnsureConstraints will, for a map of labels and properties, check whether a constraint exists for a given property on a given label, and
// if missing will create one. Creating the unique constraint ensures an index automatically.
func EnsureConstraints(im IndexManager, indexes map[string]string, log *logger.UPPLogger) error {
	// log is an optional parameter
	if log == nil {
		log = logger.NewUPPInfoLogger("neo-utils-go")
	}
	for label, propertyName := range indexes {
		err := ensureConstraint(im, label, propertyName, log)
		if err != nil { // stop as soon as something goes wrong
			return err
		}
	}
	return nil
}

func ensureIndex(im IndexManager, label string, propertyName string, log *logger.UPPLogger) error {
	indexes, err := im.Indexes(label)

	var indexFound bool

	if err != nil {
		return err
	}

	for _, index := range indexes {
		if len(index.Properties) == 1 && index.Properties[0] == propertyName {
			indexFound = true
			break
		}
	}

	if !indexFound {
		log.Infof("Creating index for type %s on property %s\n", label, propertyName)
		err := im.CreateIndex(label, propertyName)
		if err != nil {
			return err
		}
	}
	return nil

}

func ensureConstraint(im IndexManager, label string, propertyName string, log *logger.UPPLogger) error {
	cs, err := im.UniqueConstraints(label, propertyName)
	if err != nil {
		return err
	}
	if len(cs) == 0 {
		log.Infof("Creating unique constraint for type %s on property %s\n", label, propertyName)
		err = im.CreateUniqueConstraint(label, propertyName)
		if err != nil {
			return fmt.Errorf("cannot create constraint for type %s on property %s\n:, %w", label, propertyName, err)
		}
	}

	return nil

}

// IndexManager manages the maintenance of indexes and unique constraints
type IndexManager interface {
	CreateIndex(label string, propertyName string) error
	Indexes(label string) ([]Index, error)
	CreateUniqueConstraint(label string, propertyName string) error
	UniqueConstraints(label string, propertyName string) ([]UniqueConstraint, error)
}

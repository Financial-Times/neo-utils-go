package neoutils

import (
	"github.com/jmcvetta/neoism"
)

type TransactionalCypherRunner struct{ DB *neoism.Database }

func (cr TransactionalCypherRunner) String() string {
	return cr.DB.Url
}

func (cr TransactionalCypherRunner) CypherBatch(queries []*neoism.CypherQuery) error {
	tx, err := cr.DB.Begin(queries)
	if err != nil {
		if tx != nil {
			tx.Rollback()
		}
		return err
	}
	return tx.Commit()
}

package neoutils

import (
	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
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
		if err == neoism.TxQueryError {
			txErr := rwapi.ConstraintOrTransactionError{Message: err.Error()}
			for _, e := range tx.Errors {
				txErr.Details = append(txErr.Details, e.Message)
			}
			err = txErr
		}
		return err
	}
	return tx.Commit()
}

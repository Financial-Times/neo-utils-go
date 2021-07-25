package neoutils

type TransactionalCypherRunner struct{ DB Database }

func (cr TransactionalCypherRunner) String() string {
	return cr.DB.String()
}

func (cr TransactionalCypherRunner) CypherBatch(queries []*CypherQuery) error {
	return cr.DB.CypherBatch(queries)
}

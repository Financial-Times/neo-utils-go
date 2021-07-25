package neoutils

type CypherRunner interface {
	CypherBatch(queries []*CypherQuery) error
}

type IndexEnsurer interface {
	EnsureConstraints(indexes map[string]string) error
	EnsureIndexes(indexes map[string]string) error
}

type NeoConnection interface {
	CypherRunner
	IndexEnsurer
}

package neoutils

import (
	"github.com/jmcvetta/neoism"
)

type CypherRunner interface {
	CypherBatch(queries []*neoism.CypherQuery) error
}

type IndexEnsurer interface {
	EnsureConstraints(indexes map[string]string) error
	EnsureIndexes(indexes map[string]string) error
}

type NeoConnection interface {
	CypherRunner
	IndexEnsurer
}

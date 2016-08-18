package neoutils

import (
	"fmt"

	"github.com/jmcvetta/neoism"
)

type CypherRunner interface {
	CypherBatch(queries []*neoism.CypherQuery) error
}

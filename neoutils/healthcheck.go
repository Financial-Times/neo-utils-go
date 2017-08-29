package neoutils

import (
	"time"

	"github.com/jmcvetta/neoism"
)

type AsyncHealthcheck struct {
	connectionStatus error
	checkTimestamp   time.Time
	ticker           *time.Ticker
	duration         time.Duration
}

func NewAsyncHealthcheck(duration time.Duration) *AsyncHealthcheck {
	return &AsyncHealthcheck{
		duration: duration,
	}
}

func (ahc *AsyncHealthcheck) Initialise(cr CypherRunner) {
	ahc.ticker = time.NewTicker(ahc.duration)

	go func(ahc *AsyncHealthcheck) {
		for t := range ahc.ticker.C {
			ahc.checkTimestamp = t
			ahc.connectionStatus = syncCheck(cr)
		}
	}(ahc)

}

func (ahc *AsyncHealthcheck) Check() (error, time.Time) {
	return ahc.connectionStatus, ahc.checkTimestamp
}

// Check will use the supplied CypherRunner to check connectivity to Neo4j
func syncCheck(cr CypherRunner) error {
	results := []struct {
		node interface{}
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n) RETURN count(n)`,
		Result:    &results,
	}

	err := cr.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return err
	}
	return nil
}
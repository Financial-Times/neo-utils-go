package neoutils

import (
	"log"

	"github.com/jmcvetta/neoism"
)

func UnderlyingDB(con NeoConnection) *neoism.Database {
	log.Println("App is using neoism connection directly. Please update when possible to avoid this.")

	switch c := con.(type) {
	case *DefaultNeoConnection:
		return c.db
	default:
		panic("unhandled NeoConnection type")
	}

}

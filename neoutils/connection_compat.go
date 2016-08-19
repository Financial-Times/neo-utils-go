package neoutils

import (
	"fmt"

	"github.com/jmcvetta/neoism"
)

type NeoConnectionLegacy struct {
	NeoConnection
	db *neoism.Database
}

func (ncl *NeoConnectionLegacy) NeoDB() *neoism.Database {
	return ncl.db
}

func ConnectLegacy(neoURL string, conf *ConnectionConfig) (*NeoConnectionLegacy, error) {
	con, err := Connect(neoURL, conf)
	if err != nil {
		return nil, err
	}

	var db *neoism.Database

	switch c := con.(type) {
	case *DefaultNeoConnection:
		db = c.db
	default:
		return nil, fmt.Errorf("Unable to create NeoConnectionLegacy for underlying type %T", con)
	}

	return &NeoConnectionLegacy{con, db}, nil
}

/*
type legacyIndexManager struct {
	db *neoism.Database
}

func (lim *legacyIndexManager) CreateIndex(label, property string) (*neoism.Index, error) {
	return lim.db.CreateIndex(label, property)
}

func (lim *legacyIndexManager) CreateUniqueConstraint(label, property string) (*neoism.UniqueConstraint, error) {
	return lim.db.CreateUniqueConstraint(
	panic("write me")
}

func (lim *legacyIndexManager) Indexes(label string) ([]*neoism.Index, error) {
	panic("write me")
}

func (lim *legacyIndexManager) UniqueConstraints(label, property string) ([]*neoism.UniqueConstraint, error) {
	panic("write me")
}

type warnIndexManager struct {
}

func (lim *warnIndexManager) CreateIndex(label, property string) (*neoism.Index, error) {
	lim.warn()
	return nil, nil
}

func (lim *warnIndexManager) CreateUniqueConstraint(label, property string) (*neoism.UniqueConstraint, error) {
	lim.warn()
	return nil, nil
}

func (lim *warnIndexManager) Indexes(label string) ([]*neoism.Index, error) {
	lim.warn()
	return []*neoism.Index{}, nil
}

func (lim *warnIndexManager) UniqueConstraints(label, property string) ([]*neoism.UniqueConstraint, error) {
	lim.warn()
	return []*neoism.UniqueConstraint{}, nil
}

func (lim *warnIndexManager) warn() {
	log.Print("WARN : Not handling indexes or unique constraints.  Please update app to use neoutils.Connect()")
}
*/

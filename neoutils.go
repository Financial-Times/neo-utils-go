package neoutils

import (
	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
)

// StringerDb wraps neoism Database to provide a String function
type StringerDb struct{ *neoism.Database }

func (sdb StringerDb) String() string {
	return sdb.Url
}

// EnsureIndex will check whether an index exists for a given property on a given label, and if missing will create one
func EnsureIndex(db *neoism.Database, label string, property string) {

	personIndexes, err := db.Indexes(label)

	if err != nil {
		log.Errorf("Error on creating index=%v\n", err)
	}

	var indexFound bool

	for _, index := range personIndexes {
		if len(index.PropertyKeys) == 1 && index.PropertyKeys[0] == property {
			indexFound = true
			break
		}
	}
	if !indexFound {
		log.Infof("Creating index for person for neo4j instance at %s\n", db.Url)
		db.CreateIndex(label, property)
	}

}

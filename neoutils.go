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

// EnsureIndexes will, for a map of labels and properties, check whether an index exists for a given property on a given label, and if missing will create one
func EnsureIndexes(im IndexManager, indexes map[string]string) error {
	for label, propertyName := range indexes {
		err := ensureIndex(im, label, propertyName)
		if err != nil { // stop as soon as something goes wrong
			return err
		}
	}
	return nil
}

func ensureIndex(im IndexManager, label string, propertyName string) error {

	indexes, err := im.Indexes(label)

	if err != nil {
		return err
	}

	var indexFound bool

	for _, index := range indexes {
		if len(index.PropertyKeys) == 1 && index.PropertyKeys[0] == propertyName {
			indexFound = true
			break
		}
	}
	if !indexFound {
		log.Infof("Creating index for type %s on property %s\n", label, propertyName)
		_, err := im.CreateIndex(label, propertyName)
		if err != nil {
			return err
		}
	}
	return nil

}

// Manages the maintenance of indexes
type IndexManager interface {
	CreateIndex(label string, propertyName string) (*neoism.Index, error)
	Indexes(label string) ([]*neoism.Index, error)
}

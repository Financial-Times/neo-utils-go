package neoutils

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/Financial-Times/go-logger/v2"

	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
	"github.com/jmcvetta/neoism"
)

var (
	urlError          = errors.New("inappropriate url")
	notConnectedError = errors.New("not connected to neo4j database")
)

func connectAuto(neoURL string, connect func() (NeoConnection, error), delay time.Duration, log *logger.UPPLogger) (NeoConnection, error) {

	// check that at least we have a valid url
	parsed, _ := url.Parse(neoURL)
	if parsed.Host == "" {
		return nil, urlError
	}

	a := &AutoConnectTransactional{
		url:          neoURL,
		connect:      connect,
		needsConnect: make(chan struct{}, 1),
		delay:        delay,
		log:          log,
	}

	a.needsConnect <- struct{}{}

	go a.mainLoop()
	return a, nil
}

type AutoConnectTransactional struct {
	url     string
	connect func() (NeoConnection, error)
	delay   time.Duration

	lk          sync.RWMutex
	conn        NeoConnection
	indexes     []map[string]string
	constraints []map[string]string

	needsConnect chan struct{}
	log          *logger.UPPLogger
}

func (a *AutoConnectTransactional) mainLoop() {
	for {
		<-a.needsConnect

		for {
			err := a.doConnect()
			if err == nil {
				break
			}
			a.log.WithError(err).Warnf("connection to neo4j failed. Sleeping for %s", a.delay)
			time.Sleep(a.delay)
		}

		a.log.Infof("connected to %v", a.url)
	}
}

func (a *AutoConnectTransactional) doConnect() error {
	newConn, err := a.connect()
	if err != nil {
		return err
	}

	return a.postConnect(newConn)
}

// run tasks that need to happen once we get a (new) connection
func (a *AutoConnectTransactional) postConnect(newConn NeoConnection) error {
	a.lk.Lock()
	defer a.lk.Unlock()

	a.conn = newConn

	for _, i := range a.indexes {
		if err := a.conn.EnsureIndexes(i); err != nil {
			return fmt.Errorf("failed to apply indexes: %v\n", err)
		}
	}
	a.indexes = nil
	for _, c := range a.constraints {
		if err := a.conn.EnsureConstraints(c); err != nil {
			return fmt.Errorf("failed to apply constraints: %v\n", err)
		}
	}
	a.constraints = nil
	return nil
}

func (a *AutoConnectTransactional) String() string {
	return fmt.Sprintf("AutoConnectDb(%v)", a.url)
}

func (a *AutoConnectTransactional) CypherBatch(queries []*neoism.CypherQuery) error {
	a.lk.RLock()
	defer a.lk.RUnlock()
	if a.conn == nil {
		return notConnectedError
	}
	err := a.conn.CypherBatch(queries)
	if err != nil {

		needReconnect := false

		switch e := err.(type) {
		case rwapi.ConstraintOrTransactionError:
			// no reconnect needed
		case *url.Error:
			if !e.Temporary() {
				needReconnect = true
			}
		default:
			a.log.WithError(err).Warnf("unhandled error type. Assuming a reconnect is required %T", err)
			needReconnect = true
		}

		if needReconnect {
			select {
			case a.needsConnect <- struct{}{}:
				// request a reconnect
			default:
				// reconnect already queued
			}
		}

		return err
	}
	return nil
}

func (a *AutoConnectTransactional) EnsureConstraints(constraints map[string]string) error {
	a.lk.Lock()
	defer a.lk.Unlock()
	a.constraints = append(a.constraints, constraints)
	select {
	case a.needsConnect <- struct{}{}:
	default:
	}
	return nil
}

func (a *AutoConnectTransactional) EnsureIndexes(indexes map[string]string) error {
	a.lk.Lock()
	defer a.lk.Unlock()
	a.indexes = append(a.indexes, indexes)
	select {
	case a.needsConnect <- struct{}{}:
	default:
	}
	return nil
}

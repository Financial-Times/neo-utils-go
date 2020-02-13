package neoutils

import (
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/jmcvetta/neoism"
	"go4.org/osutil"
)

type ConnectionConfig struct {
	// BatchSize controls how and whether to batch multiple requests to
	// CypherQuery into a single batch. BatchSize 0 disables this behaviour.
	// Values >0 indicate the largest preferred batch size.  Actual sizes
	// may be larger because values from a single call will never be split.
	BatchSize int
	// Transactional indicates that the connection should use the
	// transactional endpoints in the neo4j REST API.
	Transactional bool
	// Optionally a custom http.Client can be supplied
	HTTPClient *http.Client
	// BackgroundConnect indicates that NeoConnection should be available when
	// neo4j is not available, and will connect and re-connect as required.
	BackgroundConnect bool
}

func DefaultConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		BatchSize:     1024,
		Transactional: true,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   60 * time.Second,
					KeepAlive: 60 * time.Second,
					DualStack: true,
				}).DialContext,
				MaxIdleConnsPerHost:   100,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second, // from DefaultTransport
				TLSHandshakeTimeout:   10 * time.Second, // from DefaultTransport
				ExpectContinueTimeout: 1 * time.Second,  // from DefaultTransport
			},
			Timeout: 60 * time.Second,
		},
		BackgroundConnect: true,
	}
}

func Connect(neoURL string, conf *ConnectionConfig, log *logger.UPPLogger) (NeoConnection, error) {
	if log == nil {
		log = logger.NewUPPInfoLogger("neo-utils-go")
	}

	if conf == nil {
		conf = DefaultConnectionConfig()
	}

	if !conf.BackgroundConnect {
		return connectDefault(neoURL, conf, log)
	} else {
		trying := make(chan struct{}, 1)
		f := func() (NeoConnection, error) {
			conn, err := connectDefault(neoURL, conf, log)
			select {
			case trying <- struct{}{}:
			default:
			}
			return conn, err
		}
		defer func() { <-trying }()
		return connectAuto(neoURL, f, 30*time.Second, log)
	}
}

func connectDefault(neoURL string, conf *ConnectionConfig, log *logger.UPPLogger) (NeoConnection, error) {

	db, err := neoism.Connect(neoURL)
	if err != nil {
		return nil, err
	}

	if conf.HTTPClient != nil {
		db.Session.Client = conf.HTTPClient
	}

	exeName, err := osutil.Executable()
	if err == nil {
		_, exeFile := filepath.Split(exeName)
		db.Session.Header.Set("User-Agent", exeFile+" (using neoutils)")
	}

	var cr CypherRunner = db
	if conf.Transactional {
		cr = TransactionalCypherRunner{db}
	} else {
		cr = db
	}

	if conf.BatchSize > 0 {
		cr = NewBatchCypherRunner(cr, conf.BatchSize)
	}

	ie := &defaultIndexEnsurer{db, log}

	return &DefaultNeoConnection{neoURL, cr, ie, db}, nil
}

type DefaultNeoConnection struct {
	dbURL string
	cr    CypherRunner
	ie    IndexEnsurer

	db *neoism.Database
}

func (c *DefaultNeoConnection) CypherBatch(cypher []*neoism.CypherQuery) error {
	return c.cr.CypherBatch(cypher)

}

func (c *DefaultNeoConnection) EnsureConstraints(constraints map[string]string) error {
	return c.ie.EnsureConstraints(constraints)
}

func (c *DefaultNeoConnection) EnsureIndexes(indexes map[string]string) error {
	return c.ie.EnsureIndexes(indexes)
}

func (c *DefaultNeoConnection) String() string {
	return fmt.Sprintf("DefaultNeoConnection(%s)", c.dbURL)
}

var _ NeoConnection = (*DefaultNeoConnection)(nil) //{}

type defaultIndexEnsurer struct {
	db  *neoism.Database
	log *logger.UPPLogger
}

func (ie *defaultIndexEnsurer) EnsureIndexes(indexes map[string]string) error {
	return EnsureIndexes(ie.db, indexes, ie.log)
}

func (ie *defaultIndexEnsurer) EnsureConstraints(constraints map[string]string) error {
	return EnsureConstraints(ie.db, constraints, ie.log)
}

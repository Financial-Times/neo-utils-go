package neoutils

import (
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger/v2"
)

var period = 100 * time.Millisecond

func TestAutoConnectBadURL(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	mock := newMockNeoConnection()
	if _, err := connectAuto("", func() (NeoConnection, error) { return mock, nil }, period, l); err == nil {
		t.Error("expected an error with bad url")
	}
	if _, err := connectAuto("foo", func() (NeoConnection, error) { return mock, nil }, period, l); err == nil {
		t.Error("expected an error with bad url")
	}
}

func TestAutoConnectInitialWithDBDown(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	_, err := connectAuto("http://valid.url/foo/bar/", func() (NeoConnection, error) { return nil, errors.New("db down") }, period, l)
	if err != nil {
		t.Errorf("didn't expect an error, despite neo being down. got %v, a %T\n", err, err)
	}
}

func TestAutoConnectConnects(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	mock := newMockNeoConnection()

	connected := make(chan struct{}, 1)

	_, err := connectAuto("http://localhost:9999/db/data/", func() (NeoConnection, error) { connected <- struct{}{}; return mock, nil }, period, l)
	if err != nil {
		t.Fatal(err)
	}

	<-connected
	// should not connect again
	select {
	case <-time.After(period + 50*time.Millisecond):
	case <-connected:
		t.Fatalf("connected again, despite no failure")
	}
}

func TestNewIndexedAreCreatedAfterConnection(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	mock := newMockNeoConnection()

	connected := make(chan struct{}, 1)

	conn, err := connectAuto("http://localhost:9999/db/data/", func() (NeoConnection, error) { connected <- struct{}{}; return mock, nil }, period, l)
	if err != nil {
		t.Fatal(err)
	}

	<-connected

	// now mandate an index
	err = conn.EnsureConstraints(map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatal(err)
	}

	// should connect again
	select {
	case <-time.After(period + 50*time.Millisecond):
		t.Fatalf("didn't connected again, despite new constraint needed")
	case <-connected:
	}
}

func TestAutoDoesIndexesAfterConnect(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	mock := newMockNeoConnection()

	allowConnect := make(chan struct{})
	connected := make(chan struct{}, 1)

	connect := func() (NeoConnection, error) {
		select {
		case <-allowConnect:
			connected <- struct{}{}
		default:
			return nil, errors.New("connection not allowed")
		}
		return mock, nil
	}

	conn, err := connectAuto("http://localhost:9999/db/data/", connect, period, l)

	if err != nil {
		t.Fatal(err)
	}

	doneIndexOrConstraints := make(chan struct{}, 2)

	mock.constraintsFunc = func(constraints map[string]string) error { doneIndexOrConstraints <- struct{}{}; return nil }
	mock.indexFunc = func(indexes map[string]string) error { doneIndexOrConstraints <- struct{}{}; return nil }

	err = conn.EnsureConstraints(map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatal(err)
	}

	err = conn.EnsureIndexes(map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatal(err)
	}

	// indexes should not have been created
	select {
	case <-doneIndexOrConstraints:
		t.Fatal("indexes created before connection")
	case <-time.After(period):
	}

	allowConnect <- struct{}{}

	// now we should have indexes within time period
	select {
	case <-time.After(period):
		t.Fatal("indexes should have been created")
	case <-doneIndexOrConstraints:
	}
	select {
	case <-time.After(period):
		t.Fatal("indexes should have been created")
	case <-doneIndexOrConstraints:
	}

	// we should see no further invocation of index or constraint create functions
	select {
	case <-time.After(period):
	case <-doneIndexOrConstraints:
		t.Fatal("indexes or constraints should not have been created")
	}

	// we are connected now
	<-connected
	// should not connect again
	select {
	case <-time.After(period + 50*time.Millisecond):
	case <-connected:
		t.Fatalf("connected again, despite no failure")
	}
}

func TestFailIndexesFailsConnect(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	mock := newMockNeoConnection()

	succeed := make(chan struct{})
	triedConnect := make(chan struct{}, 16)

	connect := func() (NeoConnection, error) {
		triedConnect <- struct{}{}
		return mock, nil
	}

	conn, err := connectAuto("http://localhost:9999/db/data/", connect, period, l)
	if err != nil {
		t.Fatal(err)
	}

	triedIndexes := make(chan struct{}, 16)

	mock.indexFunc = func(indexes map[string]string) error {
		triedIndexes <- struct{}{}
		select {
		case <-succeed:
			return nil
		default:
			return errors.New("index creation failed")
		}
	}

	err = conn.EnsureIndexes(map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatal(err)
	}

	<-triedConnect
	<-triedIndexes

	for i := 0; i < 5; i++ {
		// should try again repeatedly
		time.Sleep(period + 50*(time.Millisecond))
		select {
		case <-triedConnect:
		default:
			t.Fatal("didn't retry connect")
		}
		select {
		case <-triedIndexes:
		default:
			t.Fatal("didn't retry indexes")
		}
	}

	// allow success now
	close(succeed)
	time.Sleep(period + (50 * time.Millisecond))

	//drain
	for len(triedIndexes) > 0 {
		<-triedIndexes
	}
	for len(triedConnect) > 0 {
		<-triedConnect
	}

	// we should see no further retry
	select {
	case <-triedIndexes:
		t.Fatal("unexpected retry")
	case <-triedConnect:
		t.Fatal("unexpected retry")
	case <-time.After(period + (50 * time.Microsecond)):
	}
}

func TestFailConstraintsFailsConnect(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	mock := newMockNeoConnection()

	succeed := make(chan struct{})
	triedConnect := make(chan struct{}, 16)

	connect := func() (NeoConnection, error) {
		triedConnect <- struct{}{}
		return mock, nil
	}

	conn, err := connectAuto("http://localhost:9999/db/data/", connect, period, l)
	if err != nil {
		t.Fatal(err)
	}

	triedConstraints := make(chan struct{}, 16)

	mock.constraintsFunc = func(constraints map[string]string) error {
		triedConstraints <- struct{}{}
		select {
		case <-succeed:
			return nil
		default:
			return errors.New("constraints creation failed")
		}
	}

	err = conn.EnsureConstraints(map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatal(err)
	}

	<-triedConnect
	<-triedConstraints

	for i := 0; i < 5; i++ {
		// should try again repeatedly
		time.Sleep(period + 50*(time.Millisecond))
		select {
		case <-triedConnect:
		default:
			t.Fatal("didn't retry connect")
		}
		select {
		case <-triedConstraints:
		default:
			t.Fatal("didn't retry constraints")
		}
	}

	// allow success now
	close(succeed)
	time.Sleep(period + (50 * time.Millisecond))

	//drain
	for len(triedConstraints) > 0 {
		<-triedConstraints
	}
	for len(triedConnect) > 0 {
		<-triedConnect
	}

	// we should see no further retry
	select {
	case <-triedConstraints:
		t.Fatal("unexpected retry")
	case <-triedConnect:
		t.Fatal("unexpected retry")
	case <-time.After(period + (50 * time.Microsecond)):
	}

}

func TestCypherFailsBeforeConnected(t *testing.T) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	conn, err := connectAuto("http://valid.url/foo/bar/", func() (NeoConnection, error) { return nil, errors.New("db down") }, period, l)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(period + (50 * time.Millisecond))
	err = conn.CypherBatch([]*CypherQuery{})
	if err == nil {
		t.Error("expected error due to not being connected yet")
	}

}

func TestCypherNonTemporaryURLErrorCausesReconnect(t *testing.T) {
	testCypherErrorCausesReconnect(t, &url.Error{Op: "foo", Err: errors.New("generic error"), URL: "http://foo.bar/"}, true)
}

func TestCypherGenericErrorCausesReconnect(t *testing.T) {
	testCypherErrorCausesReconnect(t, errors.New("generic error"), true)
}

type tempError struct{}

func (pe tempError) Temporary() bool { return true }
func (pe tempError) Error() string   { return "temporary error" }

func TestCypherErrorPermanentURLErrorCausesReconnect(t *testing.T) {
	testCypherErrorCausesReconnect(t, &url.Error{Op: "foo", Err: tempError{}, URL: "http://foo.bar/"}, false)
}

func testCypherErrorCausesReconnect(t *testing.T, theError error, expectReconnect bool) {
	l := logger.NewUPPLogger("neo-utils-go-test", "PANIC")
	mock := newMockNeoConnection()
	mock.cypherFunc = func(queries []*CypherQuery) error {
		return theError
	}

	triedConnect := make(chan struct{}, 16)

	connect := func() (NeoConnection, error) {
		triedConnect <- struct{}{}
		return mock, nil
	}

	conn, err := connectAuto("http://localhost:9999/db/data/", connect, period, l)
	if err != nil {
		t.Fatal(err)
	}

	<-triedConnect
	// should not connect again
	select {
	case <-triedConnect:
		t.Fatal("should not have reconnected")
	case <-time.After(period + (50 * time.Millisecond)):
	}

	// run a failing cypher
	err = conn.CypherBatch([]*CypherQuery{})
	if err == nil {
		t.Error("expected an error")
	}
	if expectReconnect {
		// should re-connect
		select {
		case <-triedConnect:
		case <-time.After(period + (50 * time.Millisecond)):
			t.Fatal("should have reconnected")
		}
	}
	// should not connect again
	select {
	case <-triedConnect:
		t.Fatal("should not have reconnected")
	case <-time.After(period + (50 * time.Millisecond)):
	}
}

func newMockNeoConnection() *mockNeoConnection {
	return &mockNeoConnection{
		cypherFunc:      func(queries []*CypherQuery) error { return nil },
		constraintsFunc: func(constraints map[string]string) error { return nil },
		indexFunc:       func(indexes map[string]string) error { return nil },
	}
}

type mockNeoConnection struct {
	cypherFunc      func(queries []*CypherQuery) error
	constraintsFunc func(constraints map[string]string) error
	indexFunc       func(indexes map[string]string) error
}

func (m *mockNeoConnection) CypherBatch(queries []*CypherQuery) error {
	return m.cypherFunc(queries)
}

func (m *mockNeoConnection) EnsureConstraints(constraints map[string]string) error {
	return m.constraintsFunc(constraints)
}

func (m *mockNeoConnection) EnsureIndexes(indexes map[string]string) error {
	return m.indexFunc(indexes)
}

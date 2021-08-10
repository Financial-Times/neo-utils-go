package neoutils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

// Name borrowed from neoism
type Session struct {
	Client *http.Client
	Log    bool // Log request and response

	// Optional
	Userinfo *url.Userinfo

	// Optional defaults - can be overridden in a Request
	Header *http.Header
	Params *url.Values

	CommitTxEndpoint string
}

type Payload struct {
	Statements []*CypherQuery `json:"statements"`
}

// TODO: Should I use pointers instead
type NeoErrors []NeoError

func (nerrs NeoErrors) Error() string {
	return "One or more statements failed to execute, transaction was rolled back"
}

type NeoError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Record struct {
	Row []interface{} `json:"row"`
}

type NeoResult struct {
	Columns []string `json:"columns"`
	Data    []Record `json:"data"`
}

type NeoResponse struct {
	Results []NeoResult `json:"results"`
	Errors  NeoErrors   `json:"errors"`
}

func (s *Session) Send(queries ...*CypherQuery) error {
	p := Payload{
		Statements: queries,
	}
	req, err := http.NewRequest("POST", s.CommitTxEndpoint, ToReader(p))
	if err != nil {
		return err
	}
	if s.Header != nil {
		req.Header = *s.Header
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}
	var nr NeoResponse
	err = json.Unmarshal(data, &nr)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response body: %w", err)
	}

	// The transactional endpoint will return 200 or 201 status code, regardless
	// of whether statements were successfully executed. At the end of the
	// response payload, the server includes a list of errors that occurred
	// while executing statements. If the list is empty, the request completed
	// successfully.
	if len(nr.Errors) != 0 {
		return nr.Errors
	}
	queryResults, err := combineColumnsAndRows(nr.Results)
	if err != nil {
		return err
	}
	return setCypherQueryResults(p.Statements, queryResults)
}

type QueryResult []NeoObject

type NeoObject map[string]interface{}

func combineColumnsAndRows(results []NeoResult) ([]QueryResult, error) {
	queryResults := make([]QueryResult, 0, len(results))
	for _, r := range results {
		if len(r.Data) == 0 {
			queryResults = append(queryResults, QueryResult{})
			continue
		}
		if len(r.Columns) != len(r.Data[0].Row) {
			return nil, errors.New("len(Columns) is not equal to len(Row)")
		}
		qr := make(QueryResult, 0, len(r.Data))
		for _, d := range r.Data {
			nobj := NeoObject{}
			for i, c := range r.Columns {
				nobj[c] = d.Row[i]
			}
			qr = append(qr, nobj)
		}
		queryResults = append(queryResults, qr)
	}
	return queryResults, nil
}

// Each QueryResult corresponds to a query. queryResults[0] is for queries[0], etc
func setCypherQueryResults(queries []*CypherQuery, queryResults []QueryResult) error {
	if len(queries) != len(queryResults) {
		return errors.New("the number of results is not equal to the number of statements")
	}
	for i, q := range queries {
		// result not wanted
		if q.Result == nil {
			continue
		}
		qr := queryResults[i]
		data, err := json.Marshal(qr)
		if err != nil {
			return fmt.Errorf("couldn't Marshal queryResult: %w", err)
		}
		err = json.Unmarshal(data, q.Result)
		// fmt.Println(string(data), q.Result)
		if err != nil {
			return fmt.Errorf("couldn't Unmarshal into CypherQuery.Result: %w", err)
		}
	}
	return nil
}

func ToReader(v interface{}) io.Reader {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return bytes.NewReader(b)
}

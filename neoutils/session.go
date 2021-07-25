package neoutils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

// TODO: Implement Unmarshaling of the response
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
	Rows []interface{} `json:"row"`
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
	return parseResults(p.Statements, nr.Results, s.Log)
}

// Each record corresponds to a query. data[0] is for queries[0], etc
func parseResults(queries []*CypherQuery, results []NeoResult, logEnabled bool) error {
	if len(queries) != len(results) {
		return errors.New("the number of results is not equal to the number of statements")
	}
	for i, result := range results {
		if len(result.Data) == 0 || len(result.Data[0].Rows) == 0 {
			if logEnabled {
				log.Print("No data or rows")
			}
			continue
		}

		// If there is only one element returned use it, otherwise union Data and Rows
		// MATCH(s:Concept{prefUUID: abc}) RETURN s                       - [1]Data{[1]Rows}
		// MATCH(s:Concept{prefUUID: abc}) RETURN s.prefUUID, s.prefLabel - [1]Data{[2]Rows}
		// MATCH(s:Concept) RETURN s LIMIT 2                              - [2]Data{[1]Row}
		// MATCH(s:Concept) RETURN s.prefUUID, s.prefLabel LIMIT 2        - [2]Data{[2]Row}
		var row interface{}
		if len(result.Data) == 1 {
			if len(result.Data[0].Rows) == 1 {
				row = result.Data[0].Rows[0]
			} else {
				rows := []interface{}{}
				for _, r := range result.Data[0].Rows {
					rows = append(rows, r)
				}
				row = rows
			}
		} else {
			datas := []interface{}{}
			for _, d := range result.Data {
				if len(d.Rows) == 1 {
					datas = append(datas, d.Rows[0])
				} else {
					rows := []interface{}{}
					for _, r := range d.Rows {
						rows = append(rows, r)
					}
					datas = append(datas, rows)
				}
			}
			row = datas
		}

		p, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("couldn't Marshal row: %w", err)
		}
		err = json.Unmarshal(p, queries[i].Result)
		if err != nil {
			return fmt.Errorf("couldn't unmarshal to query.Result: %w", err)
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

// Package ffi adapts Cloudflare D1's HTTP API to Go's database/sql interfaces.
package ffi

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultBaseURL  = "https://api.cloudflare.com/client/v4"
	maxResponseSize = 16 << 20
)

var ErrTransactionsUnsupported = errors.New("cloudflare d1: connection-scoped transactions are unsupported; use an atomic batch")

type config struct {
	accountID  string
	databaseID string
	apiToken   string
	baseURL    string
	httpClient *http.Client
}

// Open creates a database/sql handle backed by Cloudflare's D1 HTTP API.
func Open(accountID, databaseID, apiToken string) (*sql.DB, error) {
	return open(config{
		accountID:  accountID,
		databaseID: databaseID,
		apiToken:   apiToken,
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	})
}

func open(cfg config) (*sql.DB, error) {
	if strings.TrimSpace(cfg.accountID) == "" {
		return nil, errors.New("cloudflare d1: account ID is required")
	}
	if strings.TrimSpace(cfg.databaseID) == "" {
		return nil, errors.New("cloudflare d1: database ID is required")
	}
	if strings.TrimSpace(cfg.apiToken) == "" {
		return nil, errors.New("cloudflare d1: API token is required")
	}
	if cfg.httpClient == nil {
		return nil, errors.New("cloudflare d1: HTTP client is required")
	}
	if _, err := url.ParseRequestURI(cfg.baseURL); err != nil {
		return nil, fmt.Errorf("cloudflare d1: invalid API base URL: %w", err)
	}

	db := sql.OpenDB(&connector{cfg: cfg})
	return db, nil
}

type connector struct {
	cfg config
}

func (c *connector) Connect(context.Context) (driver.Conn, error) {
	return &conn{cfg: c.cfg}, nil
}

func (c *connector) Driver() driver.Driver {
	return d1Driver{cfg: c.cfg}
}

type d1Driver struct {
	cfg config
}

func (d d1Driver) Open(string) (driver.Conn, error) {
	return &conn{cfg: d.cfg}, nil
}

type conn struct {
	cfg    config
	closed bool
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return &stmt{conn: c, query: query}, nil
}

func (c *conn) Close() error {
	c.closed = true
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, ErrTransactionsUnsupported
}

func (c *conn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return nil, ErrTransactionsUnsupported
}

func (c *conn) Ping(ctx context.Context) error {
	_, err := c.execute(ctx, "SELECT 1", nil)
	return err
}

func (c *conn) CheckNamedValue(value *driver.NamedValue) error {
	switch value.Value.(type) {
	case nil, bool, int64, float64, string, []byte, time.Time:
		return nil
	default:
		return driver.ErrSkip
	}
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	result, err := c.execute(ctx, query, namedValues(args))
	if err != nil {
		return nil, err
	}
	lastInsertID, err := numberToInt64(result.Meta.LastRowID)
	if err != nil {
		return nil, fmt.Errorf("cloudflare d1: invalid last_row_id: %w", err)
	}
	rowsAffected, err := numberToInt64(result.Meta.Changes)
	if err != nil {
		return nil, fmt.Errorf("cloudflare d1: invalid changes: %w", err)
	}
	return execResult{lastInsertID: lastInsertID, rowsAffected: rowsAffected}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	result, err := c.execute(ctx, query, namedValues(args))
	if err != nil {
		return nil, err
	}

	rows := make([][]driver.Value, len(result.Results.Rows))
	for rowIndex, source := range result.Results.Rows {
		if len(source) != len(result.Results.Columns) {
			return nil, fmt.Errorf("cloudflare d1: row %d has %d values for %d columns", rowIndex, len(source), len(result.Results.Columns))
		}
		row := make([]driver.Value, len(source))
		for columnIndex, value := range source {
			converted, err := driverValue(value)
			if err != nil {
				return nil, fmt.Errorf("cloudflare d1: row %d column %d: %w", rowIndex, columnIndex, err)
			}
			row[columnIndex] = converted
		}
		rows[rowIndex] = row
	}
	return &d1Rows{columns: result.Results.Columns, rows: rows}, nil
}

func (c *conn) execute(ctx context.Context, query string, args []any) (statementResult, error) {
	payload, err := json.Marshal(queryRequest{SQL: query, Params: requestValues(args)})
	if err != nil {
		return statementResult{}, fmt.Errorf("cloudflare d1: encode query: %w", err)
	}

	endpoint := fmt.Sprintf(
		"%s/accounts/%s/d1/database/%s/raw",
		strings.TrimRight(c.cfg.baseURL, "/"),
		url.PathEscape(c.cfg.accountID),
		url.PathEscape(c.cfg.databaseID),
	)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return statementResult{}, fmt.Errorf("cloudflare d1: create request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.cfg.apiToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "ard-cloudflare/d1")

	response, err := c.cfg.httpClient.Do(request)
	if err != nil {
		return statementResult{}, fmt.Errorf("cloudflare d1: request failed: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseSize+1))
	if err != nil {
		return statementResult{}, fmt.Errorf("cloudflare d1: read response: %w", err)
	}
	if len(body) > maxResponseSize {
		return statementResult{}, fmt.Errorf("cloudflare d1: response exceeds %d bytes", maxResponseSize)
	}

	var envelope responseEnvelope
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		return statementResult{}, fmt.Errorf("cloudflare d1: decode response (HTTP %d): %w", response.StatusCode, err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || !envelope.Success {
		return statementResult{}, apiError(response.StatusCode, envelope.Errors)
	}
	if len(envelope.Result) == 0 {
		return statementResult{}, errors.New("cloudflare d1: response contained no statement result")
	}
	if len(envelope.Result) != 1 {
		return statementResult{}, fmt.Errorf("cloudflare d1: expected one statement result, received %d; use the native batch API for multiple statements", len(envelope.Result))
	}
	if !envelope.Result[0].Success {
		if envelope.Result[0].Error != "" {
			return statementResult{}, fmt.Errorf("cloudflare d1: statement failed: %s", envelope.Result[0].Error)
		}
		return statementResult{}, errors.New("cloudflare d1: statement failed")
	}
	return envelope.Result[0], nil
}

type stmt struct {
	conn   *conn
	query  string
	closed bool
}

func (s *stmt) Close() error {
	s.closed = true
	return nil
}

func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed {
		return nil, errors.New("cloudflare d1: statement is closed")
	}
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed {
		return nil, errors.New("cloudflare d1: statement is closed")
	}
	return s.conn.QueryContext(ctx, s.query, args)
}

type execResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (r execResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r execResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

type d1Rows struct {
	columns []string
	rows    [][]driver.Value
	next    int
}

func (r *d1Rows) Columns() []string { return r.columns }
func (r *d1Rows) Close() error      { return nil }

func (r *d1Rows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

type queryRequest struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params"`
}

type responseEnvelope struct {
	Success bool              `json:"success"`
	Errors  []responseMessage `json:"errors"`
	Result  []statementResult `json:"result"`
}

type responseMessage struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type statementResult struct {
	Success bool       `json:"success"`
	Error   string     `json:"error"`
	Results rawResults `json:"results"`
	Meta    resultMeta `json:"meta"`
}

type rawResults struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

type resultMeta struct {
	Changes   json.Number `json:"changes"`
	LastRowID json.Number `json:"last_row_id"`
}

func namedValues(values []driver.NamedValue) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = value.Value
	}
	return result
}

func valuesToNamed(values []driver.Value) []driver.NamedValue {
	result := make([]driver.NamedValue, len(values))
	for index, value := range values {
		result[index] = driver.NamedValue{Ordinal: index + 1, Value: value}
	}
	return result
}

func requestValues(values []any) []any {
	result := make([]any, len(values))
	for index, value := range values {
		switch typed := value.(type) {
		case time.Time:
			result[index] = typed.Format(time.RFC3339Nano)
		case bool:
			if typed {
				result[index] = int64(1)
			} else {
				result[index] = int64(0)
			}
		case []byte:
			bytes := make([]int, len(typed))
			for i, value := range typed {
				bytes[i] = int(value)
			}
			result[index] = bytes
		default:
			result[index] = typed
		}
	}
	return result
}

func driverValue(value any) (driver.Value, error) {
	switch typed := value.(type) {
	case nil, bool, string:
		return typed, nil
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer, nil
		}
		floating, err := typed.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid numeric result %q", typed)
		}
		return floating, nil
	case []any:
		bytes := make([]byte, len(typed))
		for index, value := range typed {
			number, ok := value.(json.Number)
			if !ok {
				return nil, fmt.Errorf("unsupported array result value")
			}
			integer, err := number.Int64()
			if err != nil || integer < 0 || integer > 255 {
				return nil, fmt.Errorf("unsupported array result value")
			}
			bytes[index] = byte(integer)
		}
		return bytes, nil
	default:
		return nil, fmt.Errorf("unsupported result type %T", value)
	}
}

func numberToInt64(value json.Number) (int64, error) {
	if value == "" {
		return 0, nil
	}
	if integer, err := value.Int64(); err == nil {
		return integer, nil
	}
	floating, err := value.Float64()
	if err != nil || math.Trunc(floating) != floating || floating > math.MaxInt64 || floating < math.MinInt64 {
		return 0, fmt.Errorf("expected an integer, received %q", value)
	}
	return int64(floating), nil
}

func apiError(status int, messages []responseMessage) error {
	if len(messages) == 0 {
		return fmt.Errorf("cloudflare d1: API request failed with HTTP %d", status)
	}
	parts := make([]string, len(messages))
	for index, message := range messages {
		if message.Code == 0 {
			parts[index] = message.Message
		} else {
			parts[index] = fmt.Sprintf("%d: %s", message.Code, message.Message)
		}
	}
	return fmt.Errorf("cloudflare d1: API request failed with HTTP %d: %s", status, strings.Join(parts, "; "))
}

var (
	_ driver.Connector         = (*connector)(nil)
	_ driver.Driver            = d1Driver{}
	_ driver.Conn              = (*conn)(nil)
	_ driver.ConnBeginTx       = (*conn)(nil)
	_ driver.ExecerContext     = (*conn)(nil)
	_ driver.QueryerContext    = (*conn)(nil)
	_ driver.Pinger            = (*conn)(nil)
	_ driver.NamedValueChecker = (*conn)(nil)
	_ driver.Stmt              = (*stmt)(nil)
	_ driver.StmtExecContext   = (*stmt)(nil)
	_ driver.StmtQueryContext  = (*stmt)(nil)
)

package ffi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestQueryUsesD1RawEndpointAndReturnsRows(t *testing.T) {
	var got queryRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/accounts/account%20id/d1/database/database%2Fid/raw" {
			t.Errorf("path = %q", r.URL.EscapedPath())
		}
		if authorization := r.Header.Get("Authorization"); authorization != "Bearer secret" {
			t.Errorf("authorization = %q", authorization)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"success": true,
			"errors": [],
			"result": [{
				"success": true,
				"results": {"columns": ["id", "name"], "rows": [[42, "Ada"]]},
				"meta": {"changes": 0, "last_row_id": 0}
			}]
		}`))
	}))
	defer server.Close()

	db := openTestDB(t, server)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT id, name FROM users WHERE id = ? AND active = ?", 42, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !reflect.DeepEqual(got, queryRequest{
		SQL:    "SELECT id, name FROM users WHERE id = ? AND active = ?",
		Params: []any{float64(42), float64(1)},
	}) {
		t.Fatalf("request = %#v", got)
	}
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var id int64
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	if id != 42 || name != "Ada" {
		t.Fatalf("row = (%v, %q)", id, name)
	}
}

func TestExecReturnsD1Metadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"success": true,
			"errors": [],
			"result": [{
				"success": true,
				"results": {"columns": [], "rows": []},
				"meta": {"changes": 3, "last_row_id": 9}
			}]
		}`))
	}))
	defer server.Close()

	db := openTestDB(t, server)
	defer db.Close()
	result, err := db.Exec("DELETE FROM users")
	if err != nil {
		t.Fatal(err)
	}
	changes, _ := result.RowsAffected()
	lastID, _ := result.LastInsertId()
	if changes != 3 || lastID != 9 {
		t.Fatalf("metadata = changes %d, last ID %d", changes, lastID)
	}
}

func TestAPIErrorPreservesCloudflareDetails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{
			"success": false,
			"errors": [{"code": 10000, "message": "Authentication error"}],
			"result": []
		}`))
	}))
	defer server.Close()

	db := openTestDB(t, server)
	defer db.Close()
	_, err := db.Exec("SELECT 1")
	if err == nil || !strings.Contains(err.Error(), "10000: Authentication error") {
		t.Fatalf("error = %v", err)
	}
}

func TestRejectsMultipleStatementResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"success": true,
			"errors": [],
			"result": [
				{"success": true, "results": {"columns": [], "rows": []}, "meta": {"changes": 1}},
				{"success": true, "results": {"columns": [], "rows": []}, "meta": {"changes": 2}}
			]
		}`))
	}))
	defer server.Close()

	db := openTestDB(t, server)
	defer db.Close()
	_, err := db.Exec("DELETE FROM users; DELETE FROM sessions")
	if err == nil || !strings.Contains(err.Error(), "use the native batch API") {
		t.Fatalf("error = %v", err)
	}
}

func TestIntegerResultsPreserveValuesAboveFloatPrecision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"success": true,
			"errors": [],
			"result": [{
				"success": true,
				"results": {"columns": ["id"], "rows": [[9007199254740993]]},
				"meta": {}
			}]
		}`))
	}))
	defer server.Close()

	db := openTestDB(t, server)
	defer db.Close()
	var id int64
	if err := db.QueryRow("SELECT id FROM users").Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != 9007199254740993 {
		t.Fatalf("id = %d", id)
	}
}

func TestBeginReportsUnsupportedTransactions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	db := openTestDB(t, server)
	defer db.Close()

	_, err := db.Begin()
	if !errors.Is(err, ErrTransactionsUnsupported) {
		t.Fatalf("error = %v", err)
	}
}

func TestOpenValidatesCredentials(t *testing.T) {
	tests := []struct {
		name string
		cfg  config
		want string
	}{
		{name: "account", cfg: config{databaseID: "db", apiToken: "token", baseURL: "https://example.com", httpClient: http.DefaultClient}, want: "account ID is required"},
		{name: "database", cfg: config{accountID: "account", apiToken: "token", baseURL: "https://example.com", httpClient: http.DefaultClient}, want: "database ID is required"},
		{name: "token", cfg: config{accountID: "account", databaseID: "db", baseURL: "https://example.com", httpClient: http.DefaultClient}, want: "API token is required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := open(test.cfg)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func openTestDB(t *testing.T, server *httptest.Server) *sql.DB {
	t.Helper()
	db, err := open(config{
		accountID:  "account id",
		databaseID: "database/id",
		apiToken:   "secret",
		baseURL:    server.URL,
		httpClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

# ard-cloudflare

Ard-native clients for Cloudflare services. The first module provides D1 through the familiar [`ard-sql`](https://github.com/akonwi/ard-sql) database API.

## D1

`d1::open` adapts Cloudflare's HTTP API to Go's `database/sql`, then returns an `sql::Database`. Queries therefore use ard-sql's named parameters and `[Any]` rows, which work with [`ard-decode`](https://github.com/akonwi/ard-decode).

```ard
use cloudflare/d1

fn users() [Any]!Str {
  let db = try d1::open(d1::Config{
    account_id: "account-id",
    database_id: "database-id",
    api_token: "api-token",
  })
  defer db.close()

  db
    .query("SELECT id, name FROM users WHERE active = @active")
    .all(["active": true])
}
```

### Atomic batches

Use `d1::batch` when several statements must execute atomically. Batch statements use positional `?` parameters and results correspond to statements by index:

```ard
let results = try d1::batch(db, [
  d1::statement(
    "INSERT INTO users (id, name) VALUES (?, ?)",
    [1, "Ada"],
  ),
  d1::statement(
    "SELECT id, name FROM users WHERE id = ?",
    [1],
  ),
])

let selected_rows = results.at(1).expect("select result").rows
```

D1's HTTP API does not provide connection-scoped transactions. Calling `db.begin()` returns an unsupported-operation error rather than pretending to provide normal transaction semantics.

## Development

Validation:

```sh
ard format --check .
ard test
go test ./...
```

# ard-cloudflare

Ard-native clients for Cloudflare D1, R2, and Email Sending.

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

## R2

Create an R2 client with S3-compatible access-key credentials, then use it to upload, download, inspect, delete, and list objects.

```ard
use cloudflare/r2

let client = try r2::connect(r2::Config{
  account_id: "0123456789abcdef0123456789abcdef",
  access_key_id: "access-key-id",
  secret_access_key: "secret-access-key",
})

try r2::put_bytes(
  client,
  "assets",
  "avatars/ada.png",
  image,
  r2::PutOptions{content_type: "image/png"},
)

let object = try r2::get_bytes(client, "assets", "avatars/ada.png", Int64::from(5_000_000))
```

The initial API includes streaming uploads and downloads, bounded buffered downloads, metadata lookup, deletion, byte ranges, jurisdiction-specific endpoints, and cursor-based listing.

## Email Sending

Send structured text or HTML messages through Cloudflare Email Sending. Recipient groups, headers, and attachments are optional; at least one recipient and one body format are required.

```ard
use cloudflare/email

let client = try email::connect(email::Config{
  account_id: "0123456789abcdef0123456789abcdef",
  api_token: "api-token",
})

let result = try client.send(
  email::Message{
    from: email::Address{
      email: "login@example.com",
      name: "Maestro",
    },
    to: [email::Address{email: "ada@example.com"}],
    subject: "Sign in to Maestro",
    text: "Open the sign-in link.",
    html: "<p>Open the sign-in link.</p>",
  },
)
```

`result` includes the Cloudflare message ID and the delivered, queued, permanently bounced, and suppressed recipient lists. Attachments accept bytes and are base64-encoded for the API.

## Development

Validation:

```sh
ard format --check .
ard test
go test ./...
go vet ./...
```

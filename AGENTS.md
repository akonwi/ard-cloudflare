# AGENTS.md

## Project

Ard-native clients for Cloudflare services. Public APIs belong in Ard; small Go packages may bridge protocols or Go interfaces that Ard cannot implement ergonomically.

## Structure

- `d1.ard`: public D1 API
- `ffi/`: D1 `database/sql` adapter
- `ard.toml`: Ard package manifest
- `go.mod`: Go FFI module

## Commands

```sh
ard format --check .
ard test
go test ./...
```

## Guidelines

- Keep Cloudflare credentials out of logs and errors.
- Use tests before changing request, response, or SQL-driver behavior.
- Preserve D1-specific semantics instead of imitating unsupported `database/sql` behavior. In particular, do not emulate connection-scoped transactions; expose atomic batches explicitly.
- Bound HTTP response reads and honor request contexts.

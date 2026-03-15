# Contributing to plugin-devtools-database

## Prerequisites

- Go 1.25+
- git
- For testing: PostgreSQL, Redis, MongoDB, MySQL (any subset)

## Development

```bash
# Build
go build ./cmd/

# Test
go test ./... -v

# Vet
go vet ./...
```

## Architecture

### Adding a New Provider

1. Create `internal/db/providers/<name>.go`
2. Implement the `db.Provider` interface (20 methods)
3. Register via `init()`:
   - SQL providers: `db.RegisterProviderFactory(driver, func(sqlDB *sql.DB) db.Provider { ... })`
   - Non-SQL providers: `db.RegisterNonSQLProviderFactory(driver, func(dsn string) (db.Provider, error) { ... })`
4. Add the driver to `db_connect` schema enum and manager error message
5. Add guards to `db_export.go` and `db_import.go` if the provider doesn't support SQL export/import

### Adding Provider-Specific Tools

1. Create `internal/tools/<provider>_<name>.go` with `Schema()` and handler function
2. Create a guard helper in `<provider>_helpers.go` (see `pg_helpers.go`, `redis_helpers.go`, `mongo_helpers.go`)
3. Register the tool in `internal/plugin.go`
4. Update the tool count comment in `plugin.go`

### Tool Pattern

Every tool follows this pattern:

```go
func MyToolSchema() *structpb.Struct {
    s, _ := structpb.NewStruct(map[string]any{
        "type": "object",
        "properties": map[string]any{ ... },
        "required": []any{ ... },
    })
    return s
}

func MyTool(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
    return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
        // 1. Validate required fields
        // 2. Get provider (with type guard for provider-specific tools)
        // 3. Execute operation
        // 4. Return helpers.TextResult, helpers.JSONResult, or helpers.ErrorResult
    }
}
```

### Helper Functions

- `helpers.ValidateRequired(args, "field1", "field2")` — validates required string fields
- `helpers.ValidateOneOf(value, "opt1", "opt2")` — validates enum values
- `helpers.GetString/GetInt/GetBool/GetFloat64/GetStringSlice(args, "key")` — extract typed params
- `helpers.TextResult(msg)` — plain text response
- `helpers.JSONResult(map)` — JSON response (returns response + error)
- `helpers.ErrorResult(code, msg)` — error response

## Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `go test ./...`
5. Update docs: `README.md` and `docs/TOOLS_REFERENCE.md`
6. Submit a pull request

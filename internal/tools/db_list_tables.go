package tools

import (
	"context"
	"fmt"
	"strings"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbListTablesSchema returns the JSON Schema for the db_list_tables tool.
func DbListTablesSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID to list tables from",
			},
			"schema": map[string]any{
				"type":        "string",
				"description": "Schema name (postgres only, defaults to 'public')",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// DbListTables returns a tool handler that lists tables in a database.
func DbListTables(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		conn, err := mgr.Get(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		var rows []map[string]any

		switch conn.Driver {
		case "postgres":
			schema := helpers.GetStringOr(req.Arguments, "schema", "public")
			rows, err = mgr.Query(connID, "SELECT tablename FROM pg_tables WHERE schemaname = $1", schema)
		case "sqlite3", "sqlite":
			rows, err = mgr.Query(connID, "SELECT name FROM sqlite_master WHERE type='table'")
		case "mysql":
			rows, err = mgr.Query(connID, "SHOW TABLES")
		default:
			return helpers.ErrorResult("driver_error", fmt.Sprintf("unsupported driver: %s", conn.Driver)), nil
		}

		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		tables := make([]string, 0, len(rows))
		for _, row := range rows {
			for _, v := range row {
				if s, ok := v.(string); ok {
					tables = append(tables, s)
				}
			}
		}

		if len(tables) == 0 {
			return helpers.TextResult("No tables found."), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "Found %d tables:\n\n", len(tables))
		for _, t := range tables {
			fmt.Fprintf(&b, "- %s\n", t)
		}
		return helpers.TextResult(b.String()), nil
	}
}

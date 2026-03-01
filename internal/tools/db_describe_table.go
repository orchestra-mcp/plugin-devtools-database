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

// DbDescribeTableSchema returns the JSON Schema for the db_describe_table tool.
func DbDescribeTableSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to describe",
			},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

// DbDescribeTable returns a tool handler that shows table columns and types.
func DbDescribeTable(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")

		conn, err := mgr.Get(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		var rows []map[string]any

		switch conn.Driver {
		case "postgres":
			rows, err = mgr.Query(connID,
				"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1",
				table)
		case "sqlite3", "sqlite":
			rows, err = mgr.Query(connID, fmt.Sprintf("PRAGMA table_info(%s)", table))
		case "mysql":
			rows, err = mgr.Query(connID, fmt.Sprintf("DESCRIBE %s", table))
		default:
			return helpers.ErrorResult("driver_error", fmt.Sprintf("unsupported driver: %s", conn.Driver)), nil
		}

		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(rows) == 0 {
			return helpers.TextResult(fmt.Sprintf("Table %q not found or has no columns.", table)), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Table: %s\n\n", table)

		switch conn.Driver {
		case "postgres":
			fmt.Fprintf(&b, "| Column | Type | Nullable |\n")
			fmt.Fprintf(&b, "|--------|------|----------|\n")
			for _, row := range rows {
				fmt.Fprintf(&b, "| %v | %v | %v |\n",
					row["column_name"], row["data_type"], row["is_nullable"])
			}
		case "sqlite3", "sqlite":
			fmt.Fprintf(&b, "| CID | Name | Type | NotNull | Default | PK |\n")
			fmt.Fprintf(&b, "|-----|------|------|---------|---------|----|\n")
			for _, row := range rows {
				fmt.Fprintf(&b, "| %v | %v | %v | %v | %v | %v |\n",
					row["cid"], row["name"], row["type"],
					row["notnull"], row["dflt_value"], row["pk"])
			}
		case "mysql":
			fmt.Fprintf(&b, "| Field | Type | Null | Key | Default | Extra |\n")
			fmt.Fprintf(&b, "|-------|------|------|-----|---------|-------|\n")
			for _, row := range rows {
				fmt.Fprintf(&b, "| %v | %v | %v | %v | %v | %v |\n",
					row["Field"], row["Type"], row["Null"],
					row["Key"], row["Default"], row["Extra"])
			}
		}

		return helpers.TextResult(b.String()), nil
	}
}

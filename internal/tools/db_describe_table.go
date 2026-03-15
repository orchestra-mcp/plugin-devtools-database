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

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		columns, err := provider.DescribeTable(ctx, table)
		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(columns) == 0 {
			return helpers.TextResult(fmt.Sprintf("Table %q not found or has no columns.", table)), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Table: %s\n\n", table)
		fmt.Fprintf(&b, "| Name | Type | Nullable | Default | PK | Extra |\n")
		fmt.Fprintf(&b, "|------|------|----------|---------|----|----- |\n")
		for _, col := range columns {
			pk := ""
			if col.PrimaryKey {
				pk = "YES"
			}
			def := col.Default
			if def == "" {
				def = ""
			}
			extra := col.Extra
			if extra == "" {
				extra = ""
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n",
				col.Name, col.Type, col.Nullable, def, pk, extra)
		}
		return helpers.TextResult(b.String()), nil
	}
}

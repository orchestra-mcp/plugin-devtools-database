package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbListIndexesSchema returns the JSON Schema for the db_list_indexes tool.
func DbListIndexesSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to list indexes for",
			},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

// DbListIndexes returns a tool handler that lists indexes on a table.
func DbListIndexes(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
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

		indexes, err := provider.ListIndexes(ctx, table)
		if err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("Listing indexes is not supported by the %s provider.", provider.Kind())), nil
			}
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(indexes) == 0 {
			return helpers.TextResult(fmt.Sprintf("No indexes found on table %q.", table)), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Indexes on: %s\n\n", table)
		fmt.Fprintf(&b, "| Name | Columns | Unique |\n")
		fmt.Fprintf(&b, "|------|---------|--------|\n")
		for _, idx := range indexes {
			unique := "No"
			if idx.Unique {
				unique = "Yes"
			}
			fmt.Fprintf(&b, "| %s | %s | %s |\n", idx.Name, strings.Join(idx.Columns, ", "), unique)
		}

		return helpers.TextResult(b.String()), nil
	}
}

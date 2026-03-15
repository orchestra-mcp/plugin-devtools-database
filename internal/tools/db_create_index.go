package tools

import (
	"context"
	"errors"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbCreateIndexSchema returns the JSON Schema for the db_create_index tool.
func DbCreateIndexSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table to create the index on",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Index name",
			},
			"columns": map[string]any{
				"type":        "array",
				"description": "Column names to include in the index",
				"items":       map[string]any{"type": "string"},
			},
			"unique": map[string]any{
				"type":        "boolean",
				"description": "Create a unique index",
			},
		},
		"required": []any{"connection_id", "table", "name", "columns"},
	})
	return s
}

// DbCreateIndex returns a tool handler that creates an index on a table.
func DbCreateIndex(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		name := helpers.GetString(req.Arguments, "name")
		columns := helpers.GetStringSlice(req.Arguments, "columns")
		unique := helpers.GetBool(req.Arguments, "unique")

		if len(columns) == 0 {
			return helpers.ErrorResult("validation_error", "columns must contain at least one column name"), nil
		}

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		idx := db.IndexDef{
			Name:    name,
			Columns: columns,
			Unique:  unique,
		}

		if err := provider.CreateIndex(ctx, table, idx); err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("The %s provider does not support CREATE INDEX.", provider.Kind())), nil
			}
			return helpers.ErrorResult("create_index_error", err.Error()), nil
		}

		kind := "index"
		if unique {
			kind = "unique index"
		}
		return helpers.TextResult(fmt.Sprintf("Created %s %q on table %q (%v).", kind, name, table, columns)), nil
	}
}

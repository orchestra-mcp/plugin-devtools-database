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

// DbDropTableSchema returns the JSON Schema for the db_drop_table tool.
func DbDropTableSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to drop",
			},
			"if_exists": map[string]any{
				"type":        "boolean",
				"description": "Add IF EXISTS clause (default false)",
			},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

// DbDropTable returns a tool handler that drops (deletes) a table.
func DbDropTable(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		ifExists := helpers.GetBool(req.Arguments, "if_exists")

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		if err := provider.DropTable(ctx, table, ifExists); err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("The %s provider does not support DROP TABLE.", provider.Kind())), nil
			}
			return helpers.ErrorResult("drop_table_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Dropped table %q.", table)), nil
	}
}

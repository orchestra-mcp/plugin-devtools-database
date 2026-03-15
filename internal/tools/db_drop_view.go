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

// DbDropViewSchema returns the JSON Schema for the db_drop_view tool.
func DbDropViewSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "View name to drop",
			},
		},
		"required": []any{"connection_id", "name"},
	})
	return s
}

// DbDropView returns a tool handler that drops a database view.
func DbDropView(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		if err := provider.DropView(ctx, name); err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("The %s provider does not support DROP VIEW.", provider.Kind())), nil
			}
			return helpers.ErrorResult("drop_view_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Dropped view %q.", name)), nil
	}
}

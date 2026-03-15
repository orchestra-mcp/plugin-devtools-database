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

// DbCreateViewSchema returns the JSON Schema for the db_create_view tool.
func DbCreateViewSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "View name",
			},
			"definition": map[string]any{
				"type":        "string",
				"description": "SQL SELECT statement that defines the view",
			},
		},
		"required": []any{"connection_id", "name", "definition"},
	})
	return s
}

// DbCreateView returns a tool handler that creates a database view.
func DbCreateView(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name", "definition"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		definition := helpers.GetString(req.Arguments, "definition")

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		view := db.ViewDef{
			Name:       name,
			Definition: definition,
		}

		if err := provider.CreateView(ctx, view); err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("The %s provider does not support CREATE VIEW.", provider.Kind())), nil
			}
			return helpers.ErrorResult("create_view_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Created view %q.", name)), nil
	}
}

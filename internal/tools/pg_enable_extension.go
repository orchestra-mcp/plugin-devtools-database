package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgEnableExtensionSchema returns the JSON Schema for the pg_enable_extension tool.
func PgEnableExtensionSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Extension name to enable",
			},
			"schema": map[string]any{
				"type":        "string",
				"description": "Schema to install the extension into (optional)",
			},
		},
		"required": []any{"connection_id", "name"},
	})
	return s
}

// PgEnableExtension returns a tool handler that enables a PostgreSQL extension.
func PgEnableExtension(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		schema := helpers.GetString(req.Arguments, "schema")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.EnableExtension(ctx, name, schema); err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Extension %q enabled.", name)), nil
	}
}

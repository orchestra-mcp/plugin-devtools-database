package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgDropSchemaSchema returns the JSON Schema for the pg_drop_schema tool.
func PgDropSchemaSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Schema name to drop",
			},
			"cascade": map[string]any{
				"type":        "boolean",
				"description": "Drop all objects in the schema (CASCADE)",
			},
		},
		"required": []any{"connection_id", "name"},
	})
	return s
}

// PgDropSchema returns a tool handler that drops a PostgreSQL schema.
func PgDropSchema(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		cascade := helpers.GetBool(req.Arguments, "cascade")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.DropSchema(ctx, name, cascade); err != nil {
			return helpers.ErrorResult("schema_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Dropped schema %q.", name)), nil
	}
}

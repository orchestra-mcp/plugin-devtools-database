package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgCreateSchemaSchema returns the JSON Schema for the pg_create_schema tool.
func PgCreateSchemaSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Schema name to create",
			},
			"authorization": map[string]any{
				"type":        "string",
				"description": "Role to own the schema (optional)",
			},
		},
		"required": []any{"connection_id", "name"},
	})
	return s
}

// PgCreateSchema returns a tool handler that creates a PostgreSQL schema.
func PgCreateSchema(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		authorization := helpers.GetString(req.Arguments, "authorization")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.CreateSchema(ctx, name, authorization); err != nil {
			return helpers.ErrorResult("schema_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Created schema %q.", name)), nil
	}
}

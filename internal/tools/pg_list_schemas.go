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

// PgListSchemasSchema returns the JSON Schema for the pg_list_schemas tool.
func PgListSchemasSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgListSchemas returns a tool handler that lists PostgreSQL schemas.
func PgListSchemas(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		schemas, err := pg.ListSchemas(ctx)
		if err != nil {
			return helpers.ErrorResult("schema_error", err.Error()), nil
		}

		if len(schemas) == 0 {
			return helpers.TextResult("No schemas found."), nil
		}

		var sb strings.Builder
		sb.WriteString("| Name | Owner |\n")
		sb.WriteString("|------|-------|\n")
		for _, s := range schemas {
			sb.WriteString(fmt.Sprintf("| %s | %s |\n", s.Name, s.Owner))
		}

		return helpers.TextResult(sb.String()), nil
	}
}

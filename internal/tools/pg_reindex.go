package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgReindexSchema returns the JSON Schema for the pg_reindex tool.
func PgReindexSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"target": map[string]any{
				"type":        "string",
				"enum":        []any{"table", "index", "database"},
				"description": "Type of object to reindex",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Name of the table, index, or database to reindex",
			},
			"concurrent": map[string]any{
				"type":        "boolean",
				"description": "Reindex without locking writes (REINDEX CONCURRENTLY, PostgreSQL 12+)",
			},
		},
		"required": []any{"connection_id", "target", "name"},
	})
	return s
}

// PgReindex returns a tool handler that runs REINDEX on a PostgreSQL object.
func PgReindex(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "target", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		target := helpers.GetString(req.Arguments, "target")
		name := helpers.GetString(req.Arguments, "name")
		concurrent := helpers.GetBool(req.Arguments, "concurrent")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.Reindex(ctx, target, name, concurrent); err != nil {
			return helpers.ErrorResult("reindex_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("REINDEX completed on %s %s", target, name)), nil
	}
}

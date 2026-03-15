package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgVacuumSchema returns the JSON Schema for the pg_vacuum tool.
func PgVacuumSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to vacuum (omit to vacuum entire database)",
			},
			"analyze": map[string]any{
				"type":        "boolean",
				"description": "Also update planner statistics (VACUUM ANALYZE)",
			},
			"full": map[string]any{
				"type":        "boolean",
				"description": "Perform a full vacuum (rewrites the table, requires exclusive lock)",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgVacuum returns a tool handler that runs VACUUM on a PostgreSQL database or table.
func PgVacuum(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		analyze := helpers.GetBool(req.Arguments, "analyze")
		full := helpers.GetBool(req.Arguments, "full")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.Vacuum(ctx, table, analyze, full); err != nil {
			return helpers.ErrorResult("vacuum_error", err.Error()), nil
		}

		target := "database"
		if table != "" {
			target = fmt.Sprintf("table %s", table)
		}

		return helpers.TextResult(fmt.Sprintf("VACUUM completed on %s", target)), nil
	}
}

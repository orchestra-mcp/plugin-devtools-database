package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgAnalyzeSchema returns the JSON Schema for the pg_analyze tool.
func PgAnalyzeSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to analyze (omit to analyze entire database)",
			},
			"verbose": map[string]any{
				"type":        "boolean",
				"description": "Display progress messages (ANALYZE VERBOSE)",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgAnalyze returns a tool handler that runs ANALYZE on a PostgreSQL database or table.
func PgAnalyze(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		verbose := helpers.GetBool(req.Arguments, "verbose")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.Analyze(ctx, table, verbose); err != nil {
			return helpers.ErrorResult("analyze_error", err.Error()), nil
		}

		target := "database"
		if table != "" {
			target = fmt.Sprintf("table %s", table)
		}

		return helpers.TextResult(fmt.Sprintf("ANALYZE completed on %s", target)), nil
	}
}

package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgVectorStatsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"table":         map[string]any{"type": "string", "description": "Table name"},
			"column":        map[string]any{"type": "string", "description": "Vector column name (default: embedding)"},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

func PgVectorStats(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		column := helpers.GetString(req.Arguments, "column")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		stats, err := pg.GetVectorStats(ctx, table, column)
		if err != nil {
			return helpers.ErrorResult("vector_stats_error", err.Error()), nil
		}
		result := fmt.Sprintf("Vector Stats for %s.%s:\n- Dimensions: %d\n- Row count: %d\n- Index: %s (%s)\n- Index size: %s",
			stats.Table, stats.Column, stats.Dimensions, stats.RowCount,
			stats.IndexName, stats.IndexType, stats.IndexSize)
		if stats.IndexName == "" {
			result = fmt.Sprintf("Vector Stats for %s.%s:\n- Dimensions: %d\n- Row count: %d\n- Index: none",
				stats.Table, stats.Column, stats.Dimensions, stats.RowCount)
		}
		return helpers.TextResult(result), nil
	}
}

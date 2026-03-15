package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgCreateGINIndexSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"table":         map[string]any{"type": "string", "description": "Table name"},
			"column":        map[string]any{"type": "string", "description": "Column name (tsvector or JSONB)"},
			"index_name":    map[string]any{"type": "string", "description": "Custom index name (optional, auto-generated if empty)"},
		},
		"required": []any{"connection_id", "table", "column"},
	})
	return s
}

func PgCreateGINIndex(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "column"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		column := helpers.GetString(req.Arguments, "column")
		indexName := helpers.GetString(req.Arguments, "index_name")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.CreateGINIndex(ctx, table, column, indexName); err != nil {
			return helpers.ErrorResult("create_gin_index_error", err.Error()), nil
		}
		if indexName == "" {
			indexName = fmt.Sprintf("idx_%s_%s_gin", table, column)
		}
		return helpers.TextResult(fmt.Sprintf("Created GIN index %q on %s.%s.", indexName, table, column)), nil
	}
}

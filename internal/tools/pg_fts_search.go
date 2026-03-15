package tools

import (
	"context"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgFTSSearchSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"table":         map[string]any{"type": "string", "description": "Table name"},
			"column":        map[string]any{"type": "string", "description": "tsvector column name (default: search_vector)"},
			"query":         map[string]any{"type": "string", "description": "Search query text"},
			"language":      map[string]any{"type": "string", "description": "Text search language (default: english)"},
			"limit":         map[string]any{"type": "integer", "description": "Max results (default 20)"},
		},
		"required": []any{"connection_id", "table", "query"},
	})
	return s
}

func PgFTSSearch(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "query"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		column := helpers.GetString(req.Arguments, "column")
		query := helpers.GetString(req.Arguments, "query")
		language := helpers.GetString(req.Arguments, "language")
		limit := helpers.GetInt(req.Arguments, "limit")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		rows, err := pg.FTSSearch(ctx, table, column, query, language, limit)
		if err != nil {
			return helpers.ErrorResult("fts_search_error", err.Error()), nil
		}
		if len(rows) == 0 {
			return helpers.TextResult("No results found."), nil
		}
		return helpers.TextResult(formatQueryResults(rows)), nil
	}
}

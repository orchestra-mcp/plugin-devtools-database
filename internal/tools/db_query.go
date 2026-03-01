package tools

import (
	"context"
	"encoding/json"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbQuerySchema returns the JSON Schema for the db_query tool.
func DbQuerySchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID to query",
			},
			"query": map[string]any{
				"type":        "string",
				"description": "SQL SELECT query to execute",
			},
		},
		"required": []any{"connection_id", "query"},
	})
	return s
}

// DbQuery returns a tool handler that executes a SELECT query and returns results as JSON.
func DbQuery(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "query"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		query := helpers.GetString(req.Arguments, "query")

		rows, err := mgr.Query(connID, query)
		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		result := map[string]any{
			"rows":      rows,
			"row_count": len(rows),
		}

		raw, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return helpers.ErrorResult("marshal_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Query returned %d rows:\n\n```json\n%s\n```", len(rows), string(raw))), nil
	}
}

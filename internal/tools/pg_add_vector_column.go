package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgAddVectorColumnSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"table":         map[string]any{"type": "string", "description": "Table name"},
			"column":        map[string]any{"type": "string", "description": "Column name (default: embedding)"},
			"dimensions":    map[string]any{"type": "integer", "description": "Vector dimensions (e.g. 1536 for OpenAI, 1024 for Cohere)"},
		},
		"required": []any{"connection_id", "table", "dimensions"},
	})
	return s
}

func PgAddVectorColumn(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		column := helpers.GetString(req.Arguments, "column")
		dimensions := helpers.GetInt(req.Arguments, "dimensions")
		if dimensions <= 0 {
			return helpers.ErrorResult("validation_error", "dimensions must be a positive integer"), nil
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.AddVectorColumn(ctx, table, column, dimensions); err != nil {
			return helpers.ErrorResult("add_vector_column_error", err.Error()), nil
		}
		if column == "" {
			column = "embedding"
		}
		return helpers.TextResult(fmt.Sprintf("Added vector(%d) column %q to table %q.", dimensions, column, table)), nil
	}
}

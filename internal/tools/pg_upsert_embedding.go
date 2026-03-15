package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgUpsertEmbeddingSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id":   map[string]any{"type": "string", "description": "Connection ID"},
			"table":           map[string]any{"type": "string", "description": "Table name"},
			"id_column":       map[string]any{"type": "string", "description": "Primary key column name"},
			"id_value":        map[string]any{"type": "string", "description": "Primary key value"},
			"column":          map[string]any{"type": "string", "description": "Vector column name (default: embedding)"},
			"vector":          map[string]any{"type": "array", "items": map[string]any{"type": "number"}, "description": "Embedding vector as array of floats"},
			"metadata_column": map[string]any{"type": "string", "description": "Optional JSONB metadata column name"},
			"metadata":        map[string]any{"type": "object", "description": "Optional metadata key-value pairs"},
		},
		"required": []any{"connection_id", "table", "id_column", "id_value", "vector"},
	})
	return s
}

func PgUpsertEmbedding(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "id_column", "id_value"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		idColumn := helpers.GetString(req.Arguments, "id_column")
		idValue := helpers.GetString(req.Arguments, "id_value")
		column := helpers.GetString(req.Arguments, "column")
		metadataCol := helpers.GetString(req.Arguments, "metadata_column")

		vector, err := getFloatArray(req.Arguments, "vector")
		if err != nil || len(vector) == 0 {
			return helpers.ErrorResult("validation_error", "vector must be a non-empty array of numbers"), nil
		}

		// Parse metadata map.
		var metadata map[string]any
		if v, ok := req.Arguments.Fields["metadata"]; ok && v.GetStructValue() != nil {
			metadata = v.GetStructValue().AsMap()
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.UpsertEmbedding(ctx, table, idColumn, idValue, column, vector, metadataCol, metadata); err != nil {
			return helpers.ErrorResult("upsert_embedding_error", err.Error()), nil
		}
		return helpers.TextResult(fmt.Sprintf("Upserted embedding for %s=%q in table %q.", idColumn, idValue, table)), nil
	}
}

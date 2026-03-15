package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db/providers"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgBulkUpsertEmbeddingsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"table":         map[string]any{"type": "string", "description": "Table name"},
			"id_column":     map[string]any{"type": "string", "description": "Primary key column name"},
			"column":        map[string]any{"type": "string", "description": "Vector column name (default: embedding)"},
			"rows": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":     map[string]any{"type": "string", "description": "Row ID"},
						"vector": map[string]any{"type": "array", "items": map[string]any{"type": "number"}, "description": "Embedding vector"},
					},
				},
				"description": "Array of {id, vector} objects to upsert",
			},
		},
		"required": []any{"connection_id", "table", "id_column", "rows"},
	})
	return s
}

func PgBulkUpsertEmbeddings(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "id_column"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		idColumn := helpers.GetString(req.Arguments, "id_column")
		column := helpers.GetString(req.Arguments, "column")

		// Parse rows array.
		rowsField, ok := req.Arguments.Fields["rows"]
		if !ok || rowsField.GetListValue() == nil {
			return helpers.ErrorResult("validation_error", "rows must be a non-empty array"), nil
		}

		var rows []providers.EmbeddingRow
		for _, item := range rowsField.GetListValue().Values {
			obj := item.GetStructValue()
			if obj == nil {
				continue
			}
			id := ""
			if v, ok := obj.Fields["id"]; ok {
				id = v.GetStringValue()
			}
			var vector []float64
			if v, ok := obj.Fields["vector"]; ok && v.GetListValue() != nil {
				for _, n := range v.GetListValue().Values {
					vector = append(vector, n.GetNumberValue())
				}
			}
			if id != "" && len(vector) > 0 {
				rows = append(rows, providers.EmbeddingRow{ID: id, Vector: vector})
			}
		}
		if len(rows) == 0 {
			return helpers.ErrorResult("validation_error", "no valid rows provided"), nil
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.BulkUpsertEmbeddings(ctx, table, idColumn, column, rows); err != nil {
			return helpers.ErrorResult("bulk_upsert_error", err.Error()), nil
		}
		return helpers.TextResult(fmt.Sprintf("Bulk upserted %d embeddings in table %q.", len(rows), table)), nil
	}
}

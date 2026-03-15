package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgDeleteEmbeddingsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"table":         map[string]any{"type": "string", "description": "Table name"},
			"id_column":     map[string]any{"type": "string", "description": "Primary key column name"},
			"ids":           map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Array of IDs to delete"},
			"where":         map[string]any{"type": "string", "description": "Alternative: WHERE filter (raw SQL)"},
		},
		"required": []any{"connection_id", "table", "id_column"},
	})
	return s
}

func PgDeleteEmbeddings(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "id_column"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		idColumn := helpers.GetString(req.Arguments, "id_column")
		where := helpers.GetString(req.Arguments, "where")

		// Parse ids array.
		var ids []string
		if v, ok := req.Arguments.Fields["ids"]; ok && v.GetListValue() != nil {
			for _, item := range v.GetListValue().Values {
				if s := item.GetStringValue(); s != "" {
					ids = append(ids, s)
				}
			}
		}

		if len(ids) == 0 && where == "" {
			return helpers.ErrorResult("validation_error", "either ids or where filter is required"), nil
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		deleted, err := pg.DeleteEmbeddings(ctx, table, idColumn, ids, where)
		if err != nil {
			return helpers.ErrorResult("delete_embeddings_error", err.Error()), nil
		}
		return helpers.TextResult(fmt.Sprintf("Deleted %d rows from table %q.", deleted, table)), nil
	}
}

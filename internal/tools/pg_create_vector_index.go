package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgCreateVectorIndexSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id":   map[string]any{"type": "string", "description": "Connection ID"},
			"table":           map[string]any{"type": "string", "description": "Table name"},
			"column":          map[string]any{"type": "string", "description": "Vector column name (default: embedding)"},
			"method":          map[string]any{"type": "string", "description": "Index method: hnsw (default) or ivfflat"},
			"distance":        map[string]any{"type": "string", "description": "Distance metric: cosine (default), l2, or ip"},
			"m":               map[string]any{"type": "integer", "description": "HNSW: max connections per layer (default 16)"},
			"ef_construction": map[string]any{"type": "integer", "description": "HNSW: size of candidate list during construction (default 64)"},
			"lists":           map[string]any{"type": "integer", "description": "IVFFlat: number of inverted lists (default 100)"},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

func PgCreateVectorIndex(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		column := helpers.GetString(req.Arguments, "column")
		method := helpers.GetString(req.Arguments, "method")
		distance := helpers.GetString(req.Arguments, "distance")
		m := helpers.GetInt(req.Arguments, "m")
		efConstruction := helpers.GetInt(req.Arguments, "ef_construction")
		lists := helpers.GetInt(req.Arguments, "lists")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.CreateVectorIndex(ctx, table, column, method, distance, m, efConstruction, lists); err != nil {
			return helpers.ErrorResult("create_vector_index_error", err.Error()), nil
		}
		if method == "" {
			method = "hnsw"
		}
		if distance == "" {
			distance = "cosine"
		}
		return helpers.TextResult(fmt.Sprintf("Created %s index on %q.%q using %s distance.", method, table, column, distance)), nil
	}
}

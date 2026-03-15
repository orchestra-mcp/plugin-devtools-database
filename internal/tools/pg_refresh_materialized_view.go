package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgRefreshMaterializedViewSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"name":          map[string]any{"type": "string", "description": "Materialized view name"},
			"concurrently":  map[string]any{"type": "boolean", "description": "Refresh concurrently (requires unique index, default false)"},
		},
		"required": []any{"connection_id", "name"},
	})
	return s
}

func PgRefreshMaterializedView(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		concurrently := helpers.GetBool(req.Arguments, "concurrently")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.RefreshMaterializedView(ctx, name, concurrently); err != nil {
			return helpers.ErrorResult("refresh_matview_error", err.Error()), nil
		}
		msg := fmt.Sprintf("Refreshed materialized view %q.", name)
		if concurrently {
			msg = fmt.Sprintf("Refreshed materialized view %q concurrently.", name)
		}
		return helpers.TextResult(msg), nil
	}
}

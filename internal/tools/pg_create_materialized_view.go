package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgCreateMaterializedViewSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"name":          map[string]any{"type": "string", "description": "Materialized view name"},
			"query":         map[string]any{"type": "string", "description": "SQL SELECT query for the view"},
			"with_data":     map[string]any{"type": "boolean", "description": "Populate data immediately (default true)"},
		},
		"required": []any{"connection_id", "name", "query"},
	})
	return s
}

func PgCreateMaterializedView(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name", "query"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		query := helpers.GetString(req.Arguments, "query")
		withData := true
		if v, ok := req.Arguments.Fields["with_data"]; ok {
			withData = v.GetBoolValue()
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.CreateMaterializedView(ctx, name, query, withData); err != nil {
			return helpers.ErrorResult("create_matview_error", err.Error()), nil
		}
		msg := fmt.Sprintf("Created materialized view %q.", name)
		if !withData {
			msg += " (WITH NO DATA — run pg_refresh_materialized_view to populate)"
		}
		return helpers.TextResult(msg), nil
	}
}

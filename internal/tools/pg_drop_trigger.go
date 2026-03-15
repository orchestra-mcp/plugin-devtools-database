package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgDropTriggerSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"table":         map[string]any{"type": "string", "description": "Table the trigger belongs to"},
			"name":          map[string]any{"type": "string", "description": "Trigger name to drop"},
			"cascade":       map[string]any{"type": "boolean", "description": "Drop dependent objects (CASCADE)"},
		},
		"required": []any{"connection_id", "table", "name"},
	})
	return s
}

func PgDropTrigger(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		name := helpers.GetString(req.Arguments, "name")
		cascade := helpers.GetBool(req.Arguments, "cascade")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.DropTrigger(ctx, table, name, cascade); err != nil {
			return helpers.ErrorResult("drop_trigger_error", err.Error()), nil
		}
		return helpers.TextResult(fmt.Sprintf("Dropped trigger %q on table %q.", name, table)), nil
	}
}

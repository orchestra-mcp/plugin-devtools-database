package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgCreateTriggerSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id":  map[string]any{"type": "string", "description": "Connection ID"},
			"table":          map[string]any{"type": "string", "description": "Table name"},
			"name":           map[string]any{"type": "string", "description": "Trigger name"},
			"timing":         map[string]any{"type": "string", "description": "BEFORE, AFTER, or INSTEAD OF", "enum": []any{"BEFORE", "AFTER", "INSTEAD OF"}},
			"events":         map[string]any{"type": "array", "description": "Events: INSERT, UPDATE, DELETE", "items": map[string]any{"type": "string"}},
			"level":          map[string]any{"type": "string", "description": "ROW or STATEMENT (default ROW)", "enum": []any{"ROW", "STATEMENT"}},
			"function":       map[string]any{"type": "string", "description": "Trigger function name to execute"},
			"when_condition": map[string]any{"type": "string", "description": "Optional WHEN condition expression"},
		},
		"required": []any{"connection_id", "table", "name", "timing", "function"},
	})
	return s
}

func PgCreateTrigger(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "name", "timing", "function"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		name := helpers.GetString(req.Arguments, "name")
		timing := helpers.GetString(req.Arguments, "timing")
		level := helpers.GetString(req.Arguments, "level")
		function := helpers.GetString(req.Arguments, "function")
		whenCond := helpers.GetString(req.Arguments, "when_condition")

		var events []string
		if eventsVal, ok := req.Arguments.Fields["events"]; ok && eventsVal.GetListValue() != nil {
			for _, v := range eventsVal.GetListValue().Values {
				if s := v.GetStringValue(); s != "" {
					events = append(events, s)
				}
			}
		}
		if len(events) == 0 {
			events = []string{"INSERT"}
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.CreateTrigger(ctx, table, name, timing, events, level, function, whenCond); err != nil {
			return helpers.ErrorResult("create_trigger_error", err.Error()), nil
		}
		return helpers.TextResult(fmt.Sprintf("Created trigger %q on table %q.", name, table)), nil
	}
}

package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgNotifySchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"channel":       map[string]any{"type": "string", "description": "Notification channel name"},
			"payload":       map[string]any{"type": "string", "description": "Optional payload message"},
		},
		"required": []any{"connection_id", "channel"},
	})
	return s
}

func PgNotify(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "channel"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		channel := helpers.GetString(req.Arguments, "channel")
		payload := helpers.GetString(req.Arguments, "payload")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.Notify(ctx, channel, payload); err != nil {
			return helpers.ErrorResult("notify_error", err.Error()), nil
		}
		msg := fmt.Sprintf("Sent NOTIFY on channel %q", channel)
		if payload != "" {
			msg += fmt.Sprintf(" with payload: %s", payload)
		}
		return helpers.TextResult(msg + "."), nil
	}
}

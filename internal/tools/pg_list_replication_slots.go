package tools

import (
	"context"
	"fmt"
	"strings"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgListReplicationSlotsSchema returns the JSON Schema for the pg_list_replication_slots tool.
func PgListReplicationSlotsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgListReplicationSlots returns a tool handler that lists PostgreSQL replication slots.
func PgListReplicationSlots(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		slots, err := pg.ListReplicationSlots(ctx)
		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(slots) == 0 {
			return helpers.TextResult("No replication slots found."), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Replication Slots\n\n")
		fmt.Fprintf(&b, "| Name | Plugin | Type | Active | Restart LSN |\n")
		fmt.Fprintf(&b, "|------|--------|------|--------|-------------|\n")
		for _, s := range slots {
			active := "No"
			if s.Active {
				active = "Yes"
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n",
				s.Name, s.Plugin, s.SlotType, active, s.RestLSN)
		}

		return helpers.TextResult(b.String()), nil
	}
}

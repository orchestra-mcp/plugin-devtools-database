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

// PgReplicationStatusSchema returns the JSON Schema for the pg_replication_status tool.
func PgReplicationStatusSchema() *structpb.Struct {
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

// PgReplicationStatus returns a tool handler that shows PostgreSQL replication status.
func PgReplicationStatus(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		stats, err := pg.ReplicationStatus(ctx)
		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(stats) == 0 {
			return helpers.TextResult("No active replication connections."), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Replication Status\n\n")
		fmt.Fprintf(&b, "| PID | User | App | Client | State | Sent LSN | Write LSN | Flush LSN | Replay LSN |\n")
		fmt.Fprintf(&b, "|-----|------|-----|--------|-------|----------|-----------|-----------|------------|\n")
		for _, s := range stats {
			fmt.Fprintf(&b, "| %d | %s | %s | %s | %s | %s | %s | %s | %s |\n",
				s.PID, s.UserName, s.AppName, s.ClientIP, s.State,
				s.SentLSN, s.WriteLSN, s.FlushLSN, s.ReplayLSN)
		}

		return helpers.TextResult(b.String()), nil
	}
}

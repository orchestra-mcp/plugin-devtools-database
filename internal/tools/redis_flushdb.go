package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisFlushDBSchema returns the JSON Schema for the redis_flushdb tool.
func RedisFlushDBSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"scope": map[string]any{
				"type":        "string",
				"description": "Flush scope: \"db\" flushes the current database, \"all\" flushes every database on the server. DESTRUCTIVE — use AskUserQuestion to confirm with the user before calling this tool.",
				"enum":        []any{"db", "all"},
			},
		},
		"required": []any{"connection_id", "scope"},
	})
	return s
}

// RedisFlushDB returns a tool handler that flushes keys from a Redis database.
// This is a DESTRUCTIVE operation. Agents should use AskUserQuestion to get
// explicit user confirmation before invoking this tool.
func RedisFlushDB(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "scope"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		scope := helpers.GetString(req.Arguments, "scope")

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		client := rp.Client()

		switch scope {
		case "db":
			if _, err := client.FlushDB(ctx).Result(); err != nil {
				return helpers.ErrorResult("flushdb_error", err.Error()), nil
			}
			return helpers.TextResult("Flushed current database"), nil

		case "all":
			if _, err := client.FlushAll(ctx).Result(); err != nil {
				return helpers.ErrorResult("flushall_error", err.Error()), nil
			}
			return helpers.TextResult("Flushed all databases"), nil

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("Unknown scope %q. Valid scopes: db, all", scope)), nil
		}
	}
}

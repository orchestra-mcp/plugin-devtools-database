package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisTTLInspectSchema returns the JSON Schema for the redis_ttl_inspect tool.
func RedisTTLInspectSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"pattern": map[string]any{
				"type":        "string",
				"description": "SCAN pattern to match keys (default \"*\")",
			},
			"count": map[string]any{
				"type":        "number",
				"description": "Maximum number of keys to inspect (default 100)",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// RedisTTLInspect returns a tool handler that scans Redis keys matching a pattern
// and reports each key's type and TTL in a JSON table.
func RedisTTLInspect(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		client := rp.Client()

		pattern := helpers.GetString(req.Arguments, "pattern")
		if pattern == "" {
			pattern = "*"
		}

		maxKeys := helpers.GetInt(req.Arguments, "count")
		if maxKeys <= 0 {
			maxKeys = 100
		}

		// Collect keys via SCAN up to the limit.
		var allKeys []string
		var cursor uint64

		for {
			keys, nextCursor, err := client.Scan(ctx, cursor, pattern, int64(maxKeys)).Result()
			if err != nil {
				return helpers.ErrorResult("scan_error", err.Error()), nil
			}
			allKeys = append(allKeys, keys...)
			cursor = nextCursor

			if cursor == 0 || len(allKeys) >= maxKeys {
				break
			}
		}

		if len(allKeys) > maxKeys {
			allKeys = allKeys[:maxKeys]
		}

		// Build the result table with type and TTL for each key.
		rows := make([]map[string]any, 0, len(allKeys))
		for _, key := range allKeys {
			keyType, err := client.Type(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("type_error",
					fmt.Sprintf("failed to get type for key %q: %s", key, err.Error())), nil
			}

			ttlDuration, err := client.TTL(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("ttl_error",
					fmt.Sprintf("failed to get TTL for key %q: %s", key, err.Error())), nil
			}

			// Redis TTL semantics:
			//   -1 → key exists but has no expiration
			//   -2 → key does not exist
			// Otherwise the duration is the remaining TTL.
			var ttlSeconds int64
			switch ttlDuration {
			case -1: // no expiry
				ttlSeconds = -1
			case -2: // key gone between SCAN and TTL
				ttlSeconds = -2
			default:
				ttlSeconds = int64(ttlDuration.Seconds())
			}

			rows = append(rows, map[string]any{
				"key":         key,
				"type":        keyType,
				"ttl_seconds": ttlSeconds,
			})
		}

		resp, err := helpers.JSONResult(rows)
		if err != nil {
			return helpers.ErrorResult("json_error", err.Error()), nil
		}
		return resp, nil
	}
}

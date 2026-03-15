package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisScanKeysSchema returns the JSON Schema for the redis_scan_keys tool.
func RedisScanKeysSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"pattern": map[string]any{
				"type":        "string",
				"description": "SCAN pattern (default \"*\")",
			},
			"count": map[string]any{
				"type":        "number",
				"description": "Max keys to scan (default 100)",
			},
			"type_filter": map[string]any{
				"type":        "string",
				"description": "Filter by key type: string, list, set, zset, hash, stream",
				"enum":        []any{"string", "list", "set", "zset", "hash", "stream"},
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// RedisScanKeys returns a tool handler that performs a deep key scan on a Redis connection.
// For each matched key it retrieves TYPE, TTL, and MEMORY USAGE, returning a JSON table.
func RedisScanKeys(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		pattern := helpers.GetString(req.Arguments, "pattern")
		if pattern == "" {
			pattern = "*"
		}
		count := helpers.GetInt(req.Arguments, "count")
		if count <= 0 {
			count = 100
		}
		typeFilter := helpers.GetString(req.Arguments, "type_filter")

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		client := rp.Client()

		// SCAN keys matching the pattern, collecting up to count keys.
		var allKeys []string
		var cursor uint64

		for {
			keys, nextCursor, err := client.Scan(ctx, cursor, pattern, int64(count)).Result()
			if err != nil {
				return helpers.ErrorResult("scan_error", err.Error()), nil
			}
			allKeys = append(allKeys, keys...)
			cursor = nextCursor

			if cursor == 0 || len(allKeys) >= count {
				break
			}
		}

		if len(allKeys) > count {
			allKeys = allKeys[:count]
		}

		// For each key, get TYPE + TTL + MEMORY USAGE.
		var results []map[string]any

		for _, key := range allKeys {
			keyType, err := client.Type(ctx, key).Result()
			if err != nil {
				// Key may have been deleted between SCAN and TYPE.
				continue
			}

			// Apply type filter if specified.
			if typeFilter != "" && keyType != typeFilter {
				continue
			}

			ttlDuration, err := client.TTL(ctx, key).Result()
			if err != nil {
				continue
			}

			var ttlSeconds int64
			switch {
			case ttlDuration == -1: // No expiry.
				ttlSeconds = -1
			case ttlDuration == -2: // Key doesn't exist.
				ttlSeconds = -2
			default:
				ttlSeconds = int64(ttlDuration.Seconds())
			}

			var memBytes int64
			mem, err := client.MemoryUsage(ctx, key).Result()
			if err != nil {
				// MEMORY USAGE can fail (e.g., key evicted, module type). Default to 0.
				memBytes = 0
			} else {
				memBytes = mem
			}

			results = append(results, map[string]any{
				"key":          key,
				"type":         keyType,
				"ttl_seconds":  ttlSeconds,
				"memory_bytes": memBytes,
			})
		}

		if results == nil {
			results = []map[string]any{}
		}

		summary := map[string]any{
			"keys":        results,
			"total":       len(results),
			"pattern":     pattern,
			"type_filter": typeFilter,
		}

		resp, err := helpers.JSONResult(summary)
		if err != nil {
			return helpers.ErrorResult("json_error", fmt.Sprintf("failed to marshal result: %s", err.Error())), nil
		}
		return resp, nil
	}
}

package tools

import (
	"context"
	"fmt"
	"time"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisKeysSchema returns the JSON Schema for the redis_keys tool.
func RedisKeysSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Key operation to perform",
				"enum":        []any{"scan", "type", "ttl", "del", "exists", "rename", "persist", "expire", "expireat"},
			},
			"pattern": map[string]any{
				"type":        "string",
				"description": "Pattern for SCAN (default \"*\")",
			},
			"key": map[string]any{
				"type":        "string",
				"description": "Key name (required for most actions except scan)",
			},
			"new_key": map[string]any{
				"type":        "string",
				"description": "New key name (for rename)",
			},
			"seconds": map[string]any{
				"type":        "number",
				"description": "TTL in seconds (for expire)",
			},
			"timestamp": map[string]any{
				"type":        "number",
				"description": "Unix timestamp (for expireat)",
			},
			"count": map[string]any{
				"type":        "number",
				"description": "SCAN count hint (default 100)",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisKeys returns a tool handler that performs key management operations on a Redis connection.
func RedisKeys(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		action := helpers.GetString(req.Arguments, "action")
		key := helpers.GetString(req.Arguments, "key")

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		client := rp.Client()

		switch action {
		case "scan":
			pattern := helpers.GetString(req.Arguments, "pattern")
			if pattern == "" {
				pattern = "*"
			}
			count := helpers.GetInt(req.Arguments, "count")
			if count <= 0 {
				count = 100
			}

			var allKeys []string
			var cursor uint64
			const maxKeys = 1000

			for {
				keys, nextCursor, err := client.Scan(ctx, cursor, pattern, int64(count)).Result()
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

			result := map[string]any{
				"keys":      allKeys,
				"count":     len(allKeys),
				"pattern":   pattern,
				"truncated": len(allKeys) >= maxKeys,
			}
			resp, err := helpers.JSONResult(result)
			if err != nil {
				return helpers.ErrorResult("json_error", err.Error()), nil
			}
			return resp, nil

		case "type":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for type action"), nil
			}
			result, err := client.Type(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("type_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Key %q is of type: %s", key, result)), nil

		case "ttl":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ttl action"), nil
			}
			result, err := client.TTL(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("ttl_error", err.Error()), nil
			}
			if result == -1 {
				return helpers.TextResult(fmt.Sprintf("Key %q has no expiration.", key)), nil
			}
			if result == -2 {
				return helpers.TextResult(fmt.Sprintf("Key %q does not exist.", key)), nil
			}
			return helpers.TextResult(fmt.Sprintf("Key %q TTL: %s (%d seconds)", key, result, int64(result.Seconds()))), nil

		case "del":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for del action"), nil
			}
			deleted, err := client.Del(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("del_error", err.Error()), nil
			}
			if deleted == 0 {
				return helpers.TextResult(fmt.Sprintf("Key %q does not exist (nothing deleted).", key)), nil
			}
			return helpers.TextResult(fmt.Sprintf("Deleted key %q.", key)), nil

		case "exists":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for exists action"), nil
			}
			exists, err := client.Exists(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("exists_error", err.Error()), nil
			}
			if exists > 0 {
				return helpers.TextResult(fmt.Sprintf("Key %q exists.", key)), nil
			}
			return helpers.TextResult(fmt.Sprintf("Key %q does not exist.", key)), nil

		case "rename":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for rename action"), nil
			}
			newKey := helpers.GetString(req.Arguments, "new_key")
			if newKey == "" {
				return helpers.ErrorResult("validation_error", "new_key is required for rename action"), nil
			}
			if err := client.Rename(ctx, key, newKey).Err(); err != nil {
				return helpers.ErrorResult("rename_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Renamed key %q to %q.", key, newKey)), nil

		case "persist":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for persist action"), nil
			}
			persisted, err := client.Persist(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("persist_error", err.Error()), nil
			}
			if !persisted {
				return helpers.TextResult(fmt.Sprintf("Key %q has no expiration or does not exist.", key)), nil
			}
			return helpers.TextResult(fmt.Sprintf("Removed expiration from key %q.", key)), nil

		case "expire":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for expire action"), nil
			}
			seconds := helpers.GetInt(req.Arguments, "seconds")
			if seconds <= 0 {
				return helpers.ErrorResult("validation_error", "seconds must be a positive integer for expire action"), nil
			}
			set, err := client.Expire(ctx, key, time.Duration(seconds)*time.Second).Result()
			if err != nil {
				return helpers.ErrorResult("expire_error", err.Error()), nil
			}
			if !set {
				return helpers.TextResult(fmt.Sprintf("Key %q does not exist (expiration not set).", key)), nil
			}
			return helpers.TextResult(fmt.Sprintf("Set expiration on key %q to %d seconds.", key, seconds)), nil

		case "expireat":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for expireat action"), nil
			}
			ts := helpers.GetInt(req.Arguments, "timestamp")
			if ts <= 0 {
				return helpers.ErrorResult("validation_error", "timestamp must be a positive unix timestamp for expireat action"), nil
			}
			t := time.Unix(int64(ts), 0)
			set, err := client.ExpireAt(ctx, key, t).Result()
			if err != nil {
				return helpers.ErrorResult("expireat_error", err.Error()), nil
			}
			if !set {
				return helpers.TextResult(fmt.Sprintf("Key %q does not exist (expiration not set).", key)), nil
			}
			return helpers.TextResult(fmt.Sprintf("Set expiration on key %q to %s (unix %d).", key, t.UTC().Format(time.RFC3339), ts)), nil

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("Unknown action %q. Valid actions: scan, type, ttl, del, exists, rename, persist, expire, expireat", action)), nil
		}
	}
}

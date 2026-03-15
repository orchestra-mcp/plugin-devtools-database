package tools

import (
	"context"
	"encoding/json"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisHashesSchema returns the JSON Schema for the redis_hashes tool.
func RedisHashesSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID for the Redis instance",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Hash operation to perform",
				"enum":        []any{"hget", "hset", "hmget", "hmset", "hgetall", "hdel", "hkeys", "hvals", "hlen", "hexists", "hincrby"},
			},
			"key": map[string]any{
				"type":        "string",
				"description": "Hash key name",
			},
			"field": map[string]any{
				"type":        "string",
				"description": "Hash field name",
			},
			"value": map[string]any{
				"type":        "string",
				"description": "Value to set",
			},
			"fields": map[string]any{
				"type":        "array",
				"description": "List of field names for HMGET",
				"items": map[string]any{
					"type": "string",
				},
			},
			"pairs": map[string]any{
				"type":        "object",
				"description": "Field-value pairs for HMSET",
			},
			"increment": map[string]any{
				"type":        "number",
				"description": "Increment value for HINCRBY",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisHashes returns a tool handler for Redis hash operations.
func RedisHashes(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		action := helpers.GetString(req.Arguments, "action")
		key := helpers.GetString(req.Arguments, "key")
		field := helpers.GetString(req.Arguments, "field")
		value := helpers.GetString(req.Arguments, "value")
		increment := helpers.GetFloat64(req.Arguments, "increment")

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		switch action {
		case "hget":
			result, err := rp.Client().HGet(ctx, key, field).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(result), nil

		case "hset":
			_, err := rp.Client().HSet(ctx, key, field, value).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Field %q set in hash %q", field, key)), nil

		case "hmget":
			fields := helpers.GetStringSlice(req.Arguments, "fields")
			if len(fields) == 0 {
				return helpers.ErrorResult("validation_error", "fields is required for hmget"), nil
			}
			result, err := rp.Client().HMGet(ctx, key, fields...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			raw, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return helpers.ErrorResult("marshal_error", err.Error()), nil
			}
			return helpers.TextResult(string(raw)), nil

		case "hmset":
			pairs := helpers.GetStringMap(req.Arguments, "pairs")
			if len(pairs) == 0 {
				return helpers.ErrorResult("validation_error", "pairs is required for hmset"), nil
			}
			args := make([]any, 0, len(pairs)*2)
			for f, v := range pairs {
				args = append(args, f, v)
			}
			_, err := rp.Client().HMSet(ctx, key, args...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Set %d fields in hash %q", len(pairs), key)), nil

		case "hgetall":
			result, err := rp.Client().HGetAll(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			raw, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return helpers.ErrorResult("marshal_error", err.Error()), nil
			}
			return helpers.TextResult(string(raw)), nil

		case "hdel":
			deleted, err := rp.Client().HDel(ctx, key, field).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Deleted %d field(s) from hash %q", deleted, key)), nil

		case "hkeys":
			result, err := rp.Client().HKeys(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			raw, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return helpers.ErrorResult("marshal_error", err.Error()), nil
			}
			return helpers.TextResult(string(raw)), nil

		case "hvals":
			result, err := rp.Client().HVals(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			raw, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return helpers.ErrorResult("marshal_error", err.Error()), nil
			}
			return helpers.TextResult(string(raw)), nil

		case "hlen":
			length, err := rp.Client().HLen(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", length)), nil

		case "hexists":
			exists, err := rp.Client().HExists(ctx, key, field).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%t", exists)), nil

		case "hincrby":
			result, err := rp.Client().HIncrBy(ctx, key, field, int64(increment)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", result)), nil

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

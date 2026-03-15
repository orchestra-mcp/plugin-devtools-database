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

// RedisStringsSchema returns the JSON Schema for the redis_strings tool.
func RedisStringsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "String command to execute",
				"enum":        []any{"get", "set", "mget", "mset", "incr", "incrby", "decr", "decrby", "append", "getrange", "strlen", "setnx", "setex"},
			},
			"key": map[string]any{
				"type":        "string",
				"description": "Key name",
			},
			"value": map[string]any{
				"type":        "string",
				"description": "Value to set",
			},
			"keys": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Keys for MGET",
			},
			"pairs": map[string]any{
				"type":        "object",
				"description": "Key-value pairs for MSET",
			},
			"increment": map[string]any{
				"type":        "number",
				"description": "Increment value for INCRBY/DECRBY",
			},
			"start": map[string]any{
				"type":        "number",
				"description": "Start offset for GETRANGE",
			},
			"end": map[string]any{
				"type":        "number",
				"description": "End offset for GETRANGE",
			},
			"seconds": map[string]any{
				"type":        "number",
				"description": "TTL in seconds for SETEX",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisStrings returns a tool handler for Redis string commands.
func RedisStrings(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		action := helpers.GetString(req.Arguments, "action")

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		client := rp.Client()

		switch action {
		case "get":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for GET"), nil
			}
			val, err := client.Get(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(val), nil

		case "set":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SET"), nil
			}
			value := helpers.GetString(req.Arguments, "value")
			_, err := client.Set(ctx, key, value, 0).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — set key %q", key)), nil

		case "mget":
			var keys []string
			if v, ok := req.Arguments.Fields["keys"]; ok && v.GetListValue() != nil {
				for _, item := range v.GetListValue().GetValues() {
					if s := item.GetStringValue(); s != "" {
						keys = append(keys, s)
					}
				}
			}
			if len(keys) == 0 {
				return helpers.ErrorResult("validation_error", "keys is required for MGET"), nil
			}
			vals, err := client.MGet(ctx, keys...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			result := make(map[string]any, len(keys))
			for i, k := range keys {
				result[k] = vals[i]
			}
			return helpers.JSONResult(result)

		case "mset":
			var pairs []any
			if v, ok := req.Arguments.Fields["pairs"]; ok && v.GetStructValue() != nil {
				for k, val := range v.GetStructValue().GetFields() {
					pairs = append(pairs, k, val.GetStringValue())
				}
			}
			if len(pairs) == 0 {
				return helpers.ErrorResult("validation_error", "pairs is required for MSET"), nil
			}
			_, err := client.MSet(ctx, pairs...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — set %d key-value pairs", len(pairs)/2)), nil

		case "incr":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for INCR"), nil
			}
			val, err := client.Incr(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", val)), nil

		case "incrby":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for INCRBY"), nil
			}
			increment := helpers.GetInt(req.Arguments, "increment")
			val, err := client.IncrBy(ctx, key, int64(increment)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", val)), nil

		case "decr":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for DECR"), nil
			}
			val, err := client.Decr(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", val)), nil

		case "decrby":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for DECRBY"), nil
			}
			increment := helpers.GetInt(req.Arguments, "increment")
			val, err := client.DecrBy(ctx, key, int64(increment)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", val)), nil

		case "append":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for APPEND"), nil
			}
			value := helpers.GetString(req.Arguments, "value")
			newLen, err := client.Append(ctx, key, value).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — new length: %d", newLen)), nil

		case "getrange":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for GETRANGE"), nil
			}
			start := helpers.GetInt(req.Arguments, "start")
			end := helpers.GetInt(req.Arguments, "end")
			val, err := client.GetRange(ctx, key, int64(start), int64(end)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(val), nil

		case "strlen":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for STRLEN"), nil
			}
			length, err := client.StrLen(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", length)), nil

		case "setnx":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SETNX"), nil
			}
			value := helpers.GetString(req.Arguments, "value")
			ok, err := client.SetNX(ctx, key, value, 0).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			if ok {
				return helpers.TextResult(fmt.Sprintf("OK — key %q set (did not exist)", key)), nil
			}
			return helpers.TextResult(fmt.Sprintf("Key %q already exists, not set", key)), nil

		case "setex":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SETEX"), nil
			}
			value := helpers.GetString(req.Arguments, "value")
			seconds := helpers.GetInt(req.Arguments, "seconds")
			if seconds <= 0 {
				return helpers.ErrorResult("validation_error", "seconds must be a positive integer for SETEX"), nil
			}
			_, err := client.SetEx(ctx, key, value, time.Duration(seconds)*time.Second).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — set key %q with TTL %ds", key, seconds)), nil

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

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

// RedisListsSchema returns the JSON Schema for the redis_lists tool.
func RedisListsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"enum":        []any{"lpush", "rpush", "lpop", "rpop", "lrange", "llen", "lindex", "linsert", "lset", "lrem"},
				"description": "List operation to perform",
			},
			"key": map[string]any{
				"type":        "string",
				"description": "List key",
			},
			"value": map[string]any{
				"type":        "string",
				"description": "Value for push/set/insert/rem operations",
			},
			"values": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Multiple values for LPUSH/RPUSH",
			},
			"start": map[string]any{
				"type":        "number",
				"description": "Start index for LRANGE (default 0)",
			},
			"stop": map[string]any{
				"type":        "number",
				"description": "Stop index for LRANGE (default -1)",
			},
			"index": map[string]any{
				"type":        "number",
				"description": "Index for LINDEX/LSET",
			},
			"pivot": map[string]any{
				"type":        "string",
				"description": "Pivot value for LINSERT",
			},
			"position": map[string]any{
				"type":        "string",
				"enum":        []any{"before", "after"},
				"description": "Position for LINSERT (before or after the pivot)",
			},
			"count": map[string]any{
				"type":        "number",
				"description": "Count for LREM (0 = all, >0 = head-to-tail, <0 = tail-to-head)",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisLists returns a tool handler for Redis list operations.
func RedisLists(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		action := helpers.GetString(req.Arguments, "action")
		key := helpers.GetString(req.Arguments, "key")
		value := helpers.GetString(req.Arguments, "value")
		pivot := helpers.GetString(req.Arguments, "pivot")
		position := helpers.GetString(req.Arguments, "position")
		index := helpers.GetInt(req.Arguments, "index")
		count := helpers.GetInt(req.Arguments, "count")

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		client := rp.Client()

		switch action {
		case "lpush", "rpush":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for "+action), nil
			}
			// Collect values: prefer "values" array, fall back to single "value".
			var vals []any
			if vs := helpers.GetStringSlice(req.Arguments, "values"); len(vs) > 0 {
				vals = make([]any, len(vs))
				for i, v := range vs {
					vals[i] = v
				}
			} else if value != "" {
				vals = []any{value}
			} else {
				return helpers.ErrorResult("validation_error", "value or values is required for "+action), nil
			}

			var n int64
			var err error
			if action == "lpush" {
				n, err = client.LPush(ctx, key, vals...).Result()
			} else {
				n, err = client.RPush(ctx, key, vals...).Result()
			}
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%s: list length is now %d", action, n)), nil

		case "lpop":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for lpop"), nil
			}
			result, err := client.LPop(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(result), nil

		case "rpop":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for rpop"), nil
			}
			result, err := client.RPop(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(result), nil

		case "lrange":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for lrange"), nil
			}
			start := int64(0)
			stop := int64(-1)
			if v, ok := req.Arguments.Fields["start"]; ok {
				start = int64(v.GetNumberValue())
			}
			if v, ok := req.Arguments.Fields["stop"]; ok {
				stop = int64(v.GetNumberValue())
			}
			result, err := client.LRange(ctx, key, start, stop).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			data, err := json.Marshal(result)
			if err != nil {
				return helpers.ErrorResult("marshal_error", err.Error()), nil
			}
			return helpers.TextResult(string(data)), nil

		case "llen":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for llen"), nil
			}
			n, err := client.LLen(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", n)), nil

		case "lindex":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for lindex"), nil
			}
			result, err := client.LIndex(ctx, key, int64(index)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(result), nil

		case "linsert":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for linsert"), nil
			}
			if pivot == "" {
				return helpers.ErrorResult("validation_error", "pivot is required for linsert"), nil
			}
			if value == "" {
				return helpers.ErrorResult("validation_error", "value is required for linsert"), nil
			}
			var n int64
			var err error
			if position == "after" {
				n, err = client.LInsertAfter(ctx, key, pivot, value).Result()
			} else {
				n, err = client.LInsertBefore(ctx, key, pivot, value).Result()
			}
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("linsert: list length is now %d", n)), nil

		case "lset":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for lset"), nil
			}
			if value == "" {
				return helpers.ErrorResult("validation_error", "value is required for lset"), nil
			}
			if err := client.LSet(ctx, key, int64(index), value).Err(); err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("lset: set index %d to %q", index, value)), nil

		case "lrem":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for lrem"), nil
			}
			if value == "" {
				return helpers.ErrorResult("validation_error", "value is required for lrem"), nil
			}
			n, err := client.LRem(ctx, key, int64(count), value).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("lrem: removed %d occurrence(s)", n)), nil

		default:
			return helpers.ErrorResult("validation_error",
				fmt.Sprintf("unknown action %q (expected lpush, rpush, lpop, rpop, lrange, llen, lindex, linsert, lset, or lrem)", action)), nil
		}
	}
}

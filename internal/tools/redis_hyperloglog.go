package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisHyperLogLogSchema returns the JSON Schema for the redis_hyperloglog tool.
func RedisHyperLogLogSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID for the Redis instance",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "HyperLogLog operation to perform",
				"enum":        []any{"pfadd", "pfcount", "pfmerge"},
			},
			"key": map[string]any{
				"type":        "string",
				"description": "HyperLogLog key",
			},
			"elements": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Elements to add for PFADD",
			},
			"keys": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Keys for PFCOUNT (multiple) or PFMERGE source keys",
			},
			"dest_key": map[string]any{
				"type":        "string",
				"description": "Destination key for PFMERGE",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisHyperLogLog returns a tool handler for Redis HyperLogLog operations.
func RedisHyperLogLog(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		action := helpers.GetString(req.Arguments, "action")
		key := helpers.GetString(req.Arguments, "key")
		destKey := helpers.GetString(req.Arguments, "dest_key")

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		client := rp.Client()

		switch action {
		case "pfadd":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for pfadd"), nil
			}
			elems := helpers.GetStringSlice(req.Arguments, "elements")
			if len(elems) == 0 {
				return helpers.ErrorResult("validation_error", "elements is required for pfadd"), nil
			}
			elements := make([]any, len(elems))
			for i, e := range elems {
				elements[i] = e
			}
			n, err := client.PFAdd(ctx, key, elements...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("pfadd: %d new element(s) added to %q", n, key)), nil

		case "pfcount":
			keys := helpers.GetStringSlice(req.Arguments, "keys")
			if len(keys) == 0 {
				if key == "" {
					return helpers.ErrorResult("validation_error", "key or keys is required for pfcount"), nil
				}
				keys = []string{key}
			}
			count, err := client.PFCount(ctx, keys...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", count)), nil

		case "pfmerge":
			if destKey == "" {
				return helpers.ErrorResult("validation_error", "dest_key is required for pfmerge"), nil
			}
			keys := helpers.GetStringSlice(req.Arguments, "keys")
			if len(keys) == 0 {
				return helpers.ErrorResult("validation_error", "keys is required for pfmerge"), nil
			}
			_, err := client.PFMerge(ctx, destKey, keys...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("pfmerge: merged %d source key(s) into %q", len(keys), destKey)), nil

		default:
			return helpers.ErrorResult("validation_error",
				fmt.Sprintf("unknown action %q (expected pfadd, pfcount, or pfmerge)", action)), nil
		}
	}
}

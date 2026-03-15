package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisSetsSchema returns the JSON Schema for the redis_sets tool.
func RedisSetsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Set command to execute",
				"enum":        []any{"sadd", "srem", "smembers", "sismember", "scard", "sunion", "sinter", "sdiff", "spop", "srandmember"},
			},
			"key": map[string]any{
				"type":        "string",
				"description": "Set key",
			},
			"member": map[string]any{
				"type":        "string",
				"description": "Member value",
			},
			"members": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Multiple members for SADD",
			},
			"keys": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Keys for SUNION/SINTER/SDIFF",
			},
			"count": map[string]any{
				"type":        "number",
				"description": "Count for SPOP/SRANDMEMBER (default 1)",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisSets returns a tool handler for Redis set commands.
func RedisSets(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
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
		case "sadd":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SADD"), nil
			}
			var members []any
			if sl := helpers.GetStringSlice(req.Arguments, "members"); len(sl) > 0 {
				for _, m := range sl {
					members = append(members, m)
				}
			} else if m := helpers.GetString(req.Arguments, "member"); m != "" {
				members = append(members, m)
			}
			if len(members) == 0 {
				return helpers.ErrorResult("validation_error", "member or members is required for SADD"), nil
			}
			added, err := client.SAdd(ctx, key, members...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — added %d member(s) to set %q", added, key)), nil

		case "srem":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SREM"), nil
			}
			member := helpers.GetString(req.Arguments, "member")
			if member == "" {
				return helpers.ErrorResult("validation_error", "member is required for SREM"), nil
			}
			removed, err := client.SRem(ctx, key, member).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — removed %d member(s) from set %q", removed, key)), nil

		case "smembers":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SMEMBERS"), nil
			}
			members, err := client.SMembers(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.JSONResult(map[string]any{
				"key":     key,
				"members": members,
				"count":   len(members),
			})

		case "sismember":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SISMEMBER"), nil
			}
			member := helpers.GetString(req.Arguments, "member")
			if member == "" {
				return helpers.ErrorResult("validation_error", "member is required for SISMEMBER"), nil
			}
			exists, err := client.SIsMember(ctx, key, member).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%v", exists)), nil

		case "scard":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SCARD"), nil
			}
			count, err := client.SCard(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", count)), nil

		case "sunion":
			keys := helpers.GetStringSlice(req.Arguments, "keys")
			if len(keys) == 0 {
				return helpers.ErrorResult("validation_error", "keys is required for SUNION"), nil
			}
			members, err := client.SUnion(ctx, keys...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.JSONResult(map[string]any{
				"members": members,
				"count":   len(members),
			})

		case "sinter":
			keys := helpers.GetStringSlice(req.Arguments, "keys")
			if len(keys) == 0 {
				return helpers.ErrorResult("validation_error", "keys is required for SINTER"), nil
			}
			members, err := client.SInter(ctx, keys...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.JSONResult(map[string]any{
				"members": members,
				"count":   len(members),
			})

		case "sdiff":
			keys := helpers.GetStringSlice(req.Arguments, "keys")
			if len(keys) == 0 {
				return helpers.ErrorResult("validation_error", "keys is required for SDIFF"), nil
			}
			members, err := client.SDiff(ctx, keys...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.JSONResult(map[string]any{
				"members": members,
				"count":   len(members),
			})

		case "spop":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SPOP"), nil
			}
			count := helpers.GetInt(req.Arguments, "count")
			if count <= 0 {
				count = 1
			}
			members, err := client.SPopN(ctx, key, int64(count)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.JSONResult(map[string]any{
				"popped": members,
				"count":  len(members),
			})

		case "srandmember":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SRANDMEMBER"), nil
			}
			count := helpers.GetInt(req.Arguments, "count")
			if count <= 0 {
				count = 1
			}
			members, err := client.SRandMemberN(ctx, key, int64(count)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.JSONResult(map[string]any{
				"members": members,
				"count":   len(members),
			})

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

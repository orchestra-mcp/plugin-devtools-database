package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisSortedSetsSchema returns the JSON Schema for the redis_sorted_sets tool.
func RedisSortedSetsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Sorted set command to execute",
				"enum":        []any{"zadd", "zrem", "zrange", "zrangebyscore", "zscore", "zcard", "zrank", "zcount", "zincrby", "zpopmin", "zpopmax"},
			},
			"key": map[string]any{
				"type":        "string",
				"description": "Sorted set key",
			},
			"member": map[string]any{
				"type":        "string",
				"description": "Member",
			},
			"score": map[string]any{
				"type":        "number",
				"description": "Score for ZADD/ZINCRBY",
			},
			"start": map[string]any{
				"type":        "string",
				"description": "Start for ZRANGE (default \"0\") or ZRANGEBYSCORE (default \"-inf\")",
			},
			"stop": map[string]any{
				"type":        "string",
				"description": "Stop for ZRANGE (default \"-1\") or ZRANGEBYSCORE (default \"+inf\")",
			},
			"min": map[string]any{
				"type":        "string",
				"description": "Min for ZCOUNT (default \"-inf\")",
			},
			"max": map[string]any{
				"type":        "string",
				"description": "Max for ZCOUNT (default \"+inf\")",
			},
			"count": map[string]any{
				"type":        "number",
				"description": "Count for ZPOPMIN/ZPOPMAX",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisSortedSets returns a tool handler for Redis sorted set commands.
func RedisSortedSets(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
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
		case "zadd":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZADD"), nil
			}
			member := helpers.GetString(req.Arguments, "member")
			if member == "" {
				return helpers.ErrorResult("validation_error", "member is required for ZADD"), nil
			}
			score := helpers.GetFloat64(req.Arguments, "score")
			added, err := client.ZAdd(ctx, key, redis.Z{Score: score, Member: member}).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("ZADD: %d element(s) added to %q", added, key)), nil

		case "zrem":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZREM"), nil
			}
			member := helpers.GetString(req.Arguments, "member")
			if member == "" {
				return helpers.ErrorResult("validation_error", "member is required for ZREM"), nil
			}
			removed, err := client.ZRem(ctx, key, member).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("ZREM: %d element(s) removed from %q", removed, key)), nil

		case "zrange":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZRANGE"), nil
			}
			startStr := helpers.GetStringOr(req.Arguments, "start", "0")
			stopStr := helpers.GetStringOr(req.Arguments, "stop", "-1")
			start, err := strconv.ParseInt(startStr, 10, 64)
			if err != nil {
				return helpers.ErrorResult("validation_error", fmt.Sprintf("invalid start value: %s", startStr)), nil
			}
			stop, err := strconv.ParseInt(stopStr, 10, 64)
			if err != nil {
				return helpers.ErrorResult("validation_error", fmt.Sprintf("invalid stop value: %s", stopStr)), nil
			}
			members, err := client.ZRangeWithScores(ctx, key, start, stop).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			result := zSliceToMaps(members)
			data, _ := json.Marshal(result)
			return helpers.TextResult(string(data)), nil

		case "zrangebyscore":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZRANGEBYSCORE"), nil
			}
			min := helpers.GetStringOr(req.Arguments, "start", "-inf")
			max := helpers.GetStringOr(req.Arguments, "stop", "+inf")
			members, err := client.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
				Min: min,
				Max: max,
			}).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			result := zSliceToMaps(members)
			data, _ := json.Marshal(result)
			return helpers.TextResult(string(data)), nil

		case "zscore":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZSCORE"), nil
			}
			member := helpers.GetString(req.Arguments, "member")
			if member == "" {
				return helpers.ErrorResult("validation_error", "member is required for ZSCORE"), nil
			}
			score, err := client.ZScore(ctx, key, member).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%g", score)), nil

		case "zcard":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZCARD"), nil
			}
			count, err := client.ZCard(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", count)), nil

		case "zrank":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZRANK"), nil
			}
			member := helpers.GetString(req.Arguments, "member")
			if member == "" {
				return helpers.ErrorResult("validation_error", "member is required for ZRANK"), nil
			}
			rank, err := client.ZRank(ctx, key, member).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", rank)), nil

		case "zcount":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZCOUNT"), nil
			}
			min := helpers.GetStringOr(req.Arguments, "min", "-inf")
			max := helpers.GetStringOr(req.Arguments, "max", "+inf")
			count, err := client.ZCount(ctx, key, min, max).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", count)), nil

		case "zincrby":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZINCRBY"), nil
			}
			member := helpers.GetString(req.Arguments, "member")
			if member == "" {
				return helpers.ErrorResult("validation_error", "member is required for ZINCRBY"), nil
			}
			score := helpers.GetFloat64(req.Arguments, "score")
			newScore, err := client.ZIncrBy(ctx, key, score, member).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%g", newScore)), nil

		case "zpopmin":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZPOPMIN"), nil
			}
			count := helpers.GetInt(req.Arguments, "count")
			if count <= 0 {
				count = 1
			}
			members, err := client.ZPopMin(ctx, key, int64(count)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			result := zSliceToMaps(members)
			data, _ := json.Marshal(result)
			return helpers.TextResult(string(data)), nil

		case "zpopmax":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for ZPOPMAX"), nil
			}
			count := helpers.GetInt(req.Arguments, "count")
			if count <= 0 {
				count = 1
			}
			members, err := client.ZPopMax(ctx, key, int64(count)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			result := zSliceToMaps(members)
			data, _ := json.Marshal(result)
			return helpers.TextResult(string(data)), nil

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

// zSliceToMaps converts a []redis.Z to a JSON-friendly []map[string]any.
func zSliceToMaps(members []redis.Z) []map[string]any {
	result := make([]map[string]any, len(members))
	for i, z := range members {
		result[i] = map[string]any{
			"member": z.Member,
			"score":  z.Score,
		}
	}
	return result
}

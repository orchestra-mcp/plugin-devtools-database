package tools

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisServerSchema returns the JSON Schema for the redis_server tool.
func RedisServerSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Server administration action to perform",
				"enum":        []any{"info", "dbsize", "config_get", "config_set", "client_list", "slowlog_get", "memory_usage", "time", "randomkey"},
			},
			"section": map[string]any{
				"type":        "string",
				"description": "INFO section (default \"all\")",
			},
			"parameter": map[string]any{
				"type":        "string",
				"description": "Config parameter name for CONFIG GET/SET",
			},
			"value": map[string]any{
				"type":        "string",
				"description": "Config value for CONFIG SET",
			},
			"key": map[string]any{
				"type":        "string",
				"description": "Key for MEMORY USAGE",
			},
			"count": map[string]any{
				"type":        "number",
				"description": "Count for SLOWLOG GET (default 10)",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisServer returns a tool handler for Redis server administration commands.
func RedisServer(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
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
		case "info":
			section := helpers.GetString(req.Arguments, "section")
			if section == "" {
				section = "all"
			}
			info, err := client.Info(ctx, section).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(info), nil

		case "dbsize":
			size, err := client.DBSize(ctx).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Database contains %d keys", size)), nil

		case "config_get":
			parameter := helpers.GetString(req.Arguments, "parameter")
			if parameter == "" {
				return helpers.ErrorResult("validation_error", "parameter is required for config_get"), nil
			}
			result, err := client.ConfigGet(ctx, parameter).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			resp, jsonErr := helpers.JSONResult(result)
			if jsonErr != nil {
				return helpers.ErrorResult("json_error", jsonErr.Error()), nil
			}
			return resp, nil

		case "config_set":
			parameter := helpers.GetString(req.Arguments, "parameter")
			if parameter == "" {
				return helpers.ErrorResult("validation_error", "parameter is required for config_set"), nil
			}
			value := helpers.GetString(req.Arguments, "value")
			if value == "" {
				return helpers.ErrorResult("validation_error", "value is required for config_set"), nil
			}
			_, err := client.ConfigSet(ctx, parameter, value).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — set config %q = %q", parameter, value)), nil

		case "client_list":
			result, err := client.ClientList(ctx).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(result), nil

		case "slowlog_get":
			count := helpers.GetInt(req.Arguments, "count")
			if count <= 0 {
				count = 10
			}
			entries, err := client.SlowLogGet(ctx, int64(count)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			logs := slowLogEntriesToMaps(entries)
			resp, jsonErr := helpers.JSONResult(map[string]any{
				"entries": logs,
				"count":   len(logs),
			})
			if jsonErr != nil {
				return helpers.ErrorResult("json_error", jsonErr.Error()), nil
			}
			return resp, nil

		case "memory_usage":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for memory_usage"), nil
			}
			bytes, err := client.MemoryUsage(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Key %q uses %d bytes", key, bytes)), nil

		case "time":
			t, err := client.Time(ctx).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(t.String()), nil

		case "randomkey":
			key, err := client.RandomKey(ctx).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(key), nil

		default:
			return helpers.ErrorResult("validation_error",
				fmt.Sprintf("Unknown action %q. Valid actions: %s", action,
					strings.Join([]string{"info", "dbsize", "config_get", "config_set", "client_list", "slowlog_get", "memory_usage", "time", "randomkey"}, ", ")),
			), nil
		}
	}
}

// slowLogEntriesToMaps converts a slice of redis.SlowLog entries to a
// JSON-friendly slice of maps.
func slowLogEntriesToMaps(entries []redis.SlowLog) []map[string]any {
	result := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		result = append(result, map[string]any{
			"id":          e.ID,
			"time":        e.Time.String(),
			"duration_us": e.Duration.Microseconds(),
			"args":        e.Args,
			"client_addr": e.ClientAddr,
			"client_name": e.ClientName,
		})
	}
	return result
}

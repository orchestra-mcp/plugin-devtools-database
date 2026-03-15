package tools

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisStreamsSchema returns the JSON Schema for the redis_streams tool.
func RedisStreamsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Stream command to execute",
				"enum":        []any{"xadd", "xread", "xrange", "xrevrange", "xlen", "xinfo_stream", "xinfo_groups", "xtrim", "xdel"},
			},
			"key": map[string]any{
				"type":        "string",
				"description": "Stream key",
			},
			"fields": map[string]any{
				"type":        "object",
				"description": "Fields for XADD (map of field:value)",
			},
			"id": map[string]any{
				"type":        "string",
				"description": "Message ID for XDEL, or stream ID for XADD (default \"*\")",
			},
			"start": map[string]any{
				"type":        "string",
				"description": "Start ID for XRANGE (default \"-\")",
			},
			"end": map[string]any{
				"type":        "string",
				"description": "End ID for XRANGE (default \"+\")",
			},
			"count": map[string]any{
				"type":        "number",
				"description": "Count limit",
			},
			"maxlen": map[string]any{
				"type":        "number",
				"description": "Max length for XTRIM",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisStreams returns a tool handler for Redis stream commands.
func RedisStreams(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
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
		case "xadd":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for XADD"), nil
			}
			fieldsMap := make(map[string]any)
			if v, ok := req.Arguments.Fields["fields"]; ok && v.GetStructValue() != nil {
				for k, val := range v.GetStructValue().GetFields() {
					fieldsMap[k] = val.GetStringValue()
				}
			}
			if len(fieldsMap) == 0 {
				return helpers.ErrorResult("validation_error", "fields is required for XADD"), nil
			}
			id := helpers.GetString(req.Arguments, "id")
			if id == "" {
				id = "*"
			}
			msgID, err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: key,
				ID:     id,
				Values: fieldsMap,
			}).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — added message %s to stream %q", msgID, key)), nil

		case "xread":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for XREAD"), nil
			}
			count := helpers.GetInt(req.Arguments, "count")
			args := &redis.XReadArgs{
				Streams: []string{key, "0"},
			}
			if count > 0 {
				args.Count = int64(count)
			}
			streams, err := client.XRead(ctx, args).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			result := convertXStreams(streams)
			return helpers.JSONResult(result)

		case "xrange":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for XRANGE"), nil
			}
			start := helpers.GetString(req.Arguments, "start")
			if start == "" {
				start = "-"
			}
			end := helpers.GetString(req.Arguments, "end")
			if end == "" {
				end = "+"
			}
			count := helpers.GetInt(req.Arguments, "count")
			var msgs []redis.XMessage
			var err error
			if count > 0 {
				msgs, err = client.XRangeN(ctx, key, start, end, int64(count)).Result()
			} else {
				msgs, err = client.XRange(ctx, key, start, end).Result()
			}
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.JSONResult(convertXMessages(msgs))

		case "xrevrange":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for XREVRANGE"), nil
			}
			start := helpers.GetString(req.Arguments, "start")
			if start == "" {
				start = "-"
			}
			end := helpers.GetString(req.Arguments, "end")
			if end == "" {
				end = "+"
			}
			count := helpers.GetInt(req.Arguments, "count")
			var msgs []redis.XMessage
			var err error
			if count > 0 {
				msgs, err = client.XRevRangeN(ctx, key, end, start, int64(count)).Result()
			} else {
				msgs, err = client.XRevRange(ctx, key, end, start).Result()
			}
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.JSONResult(convertXMessages(msgs))

		case "xlen":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for XLEN"), nil
			}
			length, err := client.XLen(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", length)), nil

		case "xinfo_stream":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for XINFO STREAM"), nil
			}
			info, err := client.XInfoStream(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			result := map[string]any{
				"length":            info.Length,
				"radix_tree_keys":   info.RadixTreeKeys,
				"radix_tree_nodes":  info.RadixTreeNodes,
				"groups":            info.Groups,
				"last_generated_id": info.LastGeneratedID,
				"first_entry":       xMessageToMap(info.FirstEntry),
				"last_entry":        xMessageToMap(info.LastEntry),
			}
			return helpers.JSONResult(result)

		case "xinfo_groups":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for XINFO GROUPS"), nil
			}
			groups, err := client.XInfoGroups(ctx, key).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			var result []map[string]any
			for _, g := range groups {
				result = append(result, map[string]any{
					"name":              g.Name,
					"consumers":         g.Consumers,
					"pending":           g.Pending,
					"last_delivered_id": g.LastDeliveredID,
				})
			}
			return helpers.JSONResult(map[string]any{"groups": result})

		case "xtrim":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for XTRIM"), nil
			}
			maxlen := helpers.GetInt(req.Arguments, "maxlen")
			if maxlen <= 0 {
				return helpers.ErrorResult("validation_error", "maxlen must be a positive integer for XTRIM"), nil
			}
			trimmed, err := client.XTrimMaxLen(ctx, key, int64(maxlen)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — trimmed %d entries from stream %q (maxlen %d)", trimmed, key, maxlen)), nil

		case "xdel":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for XDEL"), nil
			}
			id := helpers.GetString(req.Arguments, "id")
			if id == "" {
				return helpers.ErrorResult("validation_error", "id is required for XDEL"), nil
			}
			deleted, err := client.XDel(ctx, key, id).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("OK — deleted %d entries from stream %q", deleted, key)), nil

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

// convertXMessages converts a slice of redis.XMessage to []map[string]any.
func convertXMessages(msgs []redis.XMessage) []map[string]any {
	result := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		result = append(result, xMessageToMap(m))
	}
	return result
}

// xMessageToMap converts a single redis.XMessage to map[string]any.
func xMessageToMap(m redis.XMessage) map[string]any {
	values := make(map[string]any, len(m.Values))
	for k, v := range m.Values {
		values[k] = v
	}
	return map[string]any{
		"id":     m.ID,
		"values": values,
	}
}

// convertXStreams converts the result of XRead to a serializable structure.
func convertXStreams(streams []redis.XStream) []map[string]any {
	result := make([]map[string]any, 0, len(streams))
	for _, s := range streams {
		result = append(result, map[string]any{
			"stream":   s.Stream,
			"messages": convertXMessages(s.Messages),
		})
	}
	return result
}

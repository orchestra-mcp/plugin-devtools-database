package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisPubSubSchema returns the JSON Schema for the redis_pubsub tool.
func RedisPubSubSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Pub/Sub operation to perform",
				"enum":        []any{"publish", "pubsub_channels", "pubsub_numsub", "pubsub_numpat"},
			},
			"channel": map[string]any{
				"type":        "string",
				"description": "Channel name for PUBLISH",
			},
			"message": map[string]any{
				"type":        "string",
				"description": "Message for PUBLISH",
			},
			"pattern": map[string]any{
				"type":        "string",
				"description": "Pattern for PUBSUB CHANNELS (default \"*\")",
			},
			"channels": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Channels for PUBSUB NUMSUB",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisPubSub returns a tool handler for Redis Pub/Sub introspection and publishing.
// Note: SUBSCRIBE is intentionally excluded — it blocks and is not suitable for a tool-based interface.
func RedisPubSub(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
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
		case "publish":
			channel := helpers.GetString(req.Arguments, "channel")
			if channel == "" {
				return helpers.ErrorResult("validation_error", "channel is required for publish action"), nil
			}
			message := helpers.GetString(req.Arguments, "message")
			if message == "" {
				return helpers.ErrorResult("validation_error", "message is required for publish action"), nil
			}
			receivers, err := client.Publish(ctx, channel, message).Result()
			if err != nil {
				return helpers.ErrorResult("publish_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Published to %d subscribers on channel %q.", receivers, channel)), nil

		case "pubsub_channels":
			pattern := helpers.GetString(req.Arguments, "pattern")
			if pattern == "" {
				pattern = "*"
			}
			channels, err := client.PubSubChannels(ctx, pattern).Result()
			if err != nil {
				return helpers.ErrorResult("pubsub_channels_error", err.Error()), nil
			}
			result := map[string]any{
				"channels": channels,
				"count":    len(channels),
				"pattern":  pattern,
			}
			resp, err := helpers.JSONResult(result)
			if err != nil {
				return helpers.ErrorResult("json_error", err.Error()), nil
			}
			return resp, nil

		case "pubsub_numsub":
			var channels []string
			if v, ok := req.Arguments.Fields["channels"]; ok && v.GetListValue() != nil {
				for _, item := range v.GetListValue().GetValues() {
					if s := item.GetStringValue(); s != "" {
						channels = append(channels, s)
					}
				}
			}
			if len(channels) == 0 {
				return helpers.ErrorResult("validation_error", "channels array is required for pubsub_numsub action"), nil
			}
			subs, err := client.PubSubNumSub(ctx, channels...).Result()
			if err != nil {
				return helpers.ErrorResult("pubsub_numsub_error", err.Error()), nil
			}
			result := make(map[string]any, len(subs))
			for ch, count := range subs {
				result[ch] = count
			}
			resp, err := helpers.JSONResult(result)
			if err != nil {
				return helpers.ErrorResult("json_error", err.Error()), nil
			}
			return resp, nil

		case "pubsub_numpat":
			count, err := client.PubSubNumPat(ctx).Result()
			if err != nil {
				return helpers.ErrorResult("pubsub_numpat_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Active pattern subscriptions: %d", count)), nil

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("Unknown action %q. Valid actions: publish, pubsub_channels, pubsub_numsub, pubsub_numpat", action)), nil
		}
	}
}

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

// RedisBitmapSchema returns the JSON Schema for the redis_bitmap tool.
func RedisBitmapSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Bitmap command to execute",
				"enum":        []any{"setbit", "getbit", "bitcount", "bitpos", "bitop"},
			},
			"key": map[string]any{
				"type":        "string",
				"description": "Bitmap key",
			},
			"offset": map[string]any{
				"type":        "number",
				"description": "Bit offset for SETBIT/GETBIT",
			},
			"value": map[string]any{
				"type":        "number",
				"description": "Bit value (0 or 1) for SETBIT",
			},
			"start": map[string]any{
				"type":        "number",
				"description": "Start byte for BITCOUNT",
			},
			"end": map[string]any{
				"type":        "number",
				"description": "End byte for BITCOUNT",
			},
			"bit": map[string]any{
				"type":        "number",
				"description": "Bit value to search for BITPOS (0 or 1)",
			},
			"operation": map[string]any{
				"type":        "string",
				"description": "Bitwise operation for BITOP: and, or, xor, not",
			},
			"dest_key": map[string]any{
				"type":        "string",
				"description": "Destination key for BITOP",
			},
			"keys": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Source keys for BITOP",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisBitmap returns a tool handler for Redis bitmap commands.
func RedisBitmap(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
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
		case "setbit":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for SETBIT"), nil
			}
			offset := helpers.GetInt(req.Arguments, "offset")
			value := helpers.GetInt(req.Arguments, "value")
			prev, err := client.SetBit(ctx, key, int64(offset), int(value)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("SETBIT %s offset %d = %d (previous: %d)", key, offset, value, prev)), nil

		case "getbit":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for GETBIT"), nil
			}
			offset := helpers.GetInt(req.Arguments, "offset")
			val, err := client.GetBit(ctx, key, int64(offset)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", val)), nil

		case "bitcount":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for BITCOUNT"), nil
			}
			_, hasStart := req.Arguments.Fields["start"]
			_, hasEnd := req.Arguments.Fields["end"]
			var count int64
			var err error
			if hasStart || hasEnd {
				start := helpers.GetInt(req.Arguments, "start")
				end := helpers.GetInt(req.Arguments, "end")
				count, err = client.BitCount(ctx, key, &redis.BitCount{
					Start: int64(start),
					End:   int64(end),
				}).Result()
			} else {
				count, err = client.BitCount(ctx, key, nil).Result()
			}
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", count)), nil

		case "bitpos":
			key := helpers.GetString(req.Arguments, "key")
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for BITPOS"), nil
			}
			bit := helpers.GetInt(req.Arguments, "bit")
			pos, err := client.BitPos(ctx, key, int64(bit)).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%d", pos)), nil

		case "bitop":
			operation := helpers.GetString(req.Arguments, "operation")
			if operation == "" {
				return helpers.ErrorResult("validation_error", "operation is required for BITOP"), nil
			}
			destKey := helpers.GetString(req.Arguments, "dest_key")
			if destKey == "" {
				return helpers.ErrorResult("validation_error", "dest_key is required for BITOP"), nil
			}
			keys := helpers.GetStringSlice(req.Arguments, "keys")
			if len(keys) == 0 {
				return helpers.ErrorResult("validation_error", "keys is required for BITOP"), nil
			}

			var n int64
			var err error
			switch operation {
			case "and":
				n, err = client.BitOpAnd(ctx, destKey, keys...).Result()
			case "or":
				n, err = client.BitOpOr(ctx, destKey, keys...).Result()
			case "xor":
				n, err = client.BitOpXor(ctx, destKey, keys...).Result()
			case "not":
				if len(keys) != 1 {
					return helpers.ErrorResult("validation_error", "BITOP NOT requires exactly one source key"), nil
				}
				n, err = client.BitOpNot(ctx, destKey, keys[0]).Result()
			default:
				return helpers.ErrorResult("validation_error",
					fmt.Sprintf("unknown BITOP operation %q (expected and, or, xor, or not)", operation)), nil
			}
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("BITOP %s → %s: result length %d bytes", operation, destKey, n)), nil

		default:
			return helpers.ErrorResult("validation_error",
				fmt.Sprintf("unknown action %q (expected setbit, getbit, bitcount, bitpos, or bitop)", action)), nil
		}
	}
}

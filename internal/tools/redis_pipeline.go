package tools

import (
	"context"
	"fmt"
	"strings"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisPipelineSchema returns the JSON Schema for the redis_pipeline tool.
func RedisPipelineSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID for the Redis instance",
			},
			"commands": map[string]any{
				"type":        "array",
				"description": "List of Redis commands to execute in the pipeline, e.g. [\"SET foo bar\", \"GET foo\", \"INCR counter\"]",
				"items": map[string]any{
					"type": "string",
				},
			},
			"transaction": map[string]any{
				"type":        "boolean",
				"description": "If true, wrap commands in MULTI/EXEC (TxPipeline) for atomic execution; if false, use a regular pipeline",
			},
		},
		"required": []any{"connection_id", "commands"},
	})
	return s
}

// RedisPipeline returns a tool handler that executes multiple Redis commands
// in a single pipeline (or transaction) round-trip.
func RedisPipeline(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		transaction := helpers.GetBool(req.Arguments, "transaction")

		// Extract command strings from the array.
		commands := helpers.GetStringSlice(req.Arguments, "commands")
		if len(commands) == 0 {
			return helpers.ErrorResult("validation_error", "commands must be a non-empty array of strings"), nil
		}

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		client := rp.Client()

		// Create the appropriate pipeline type.
		var pipe redis.Pipeliner
		if transaction {
			pipe = client.TxPipeline()
		} else {
			pipe = client.Pipeline()
		}

		// Queue each command into the pipeline.
		cmds := make([]*redis.Cmd, 0, len(commands))
		cmdStrings := make([]string, 0, len(commands))
		for _, cmd := range commands {
			parts := strings.Fields(cmd)
			if len(parts) == 0 {
				continue
			}
			args := make([]any, len(parts))
			for i, p := range parts {
				args[i] = p
			}
			cmds = append(cmds, pipe.Do(ctx, args...))
			cmdStrings = append(cmdStrings, cmd)
		}

		// Execute the pipeline.
		_, execErr := pipe.Exec(ctx)
		// Exec returns redis.Nil when a GET in the pipeline returns nil,
		// which is not a real failure. Only report exec-level errors when
		// all individual commands succeeded (meaning it is a transport or
		// transaction-level failure).
		if execErr != nil && execErr != redis.Nil {
			allIndividualOK := true
			for _, c := range cmds {
				if c.Err() != nil && c.Err() != redis.Nil {
					allIndividualOK = false
					break
				}
			}
			if allIndividualOK {
				kind := "pipeline"
				if transaction {
					kind = "transaction"
				}
				return helpers.ErrorResult("pipeline_error",
					fmt.Sprintf("%s exec failed: %s", kind, execErr.Error())), nil
			}
		}

		// Collect results from each command.
		results := make([]map[string]any, 0, len(cmds))
		for i, c := range cmds {
			entry := map[string]any{
				"command": cmdStrings[i],
			}
			if c.Err() != nil && c.Err() != redis.Nil {
				entry["result"] = nil
				entry["error"] = c.Err().Error()
			} else {
				entry["result"] = fmt.Sprintf("%v", c.Val())
				entry["error"] = nil
			}
			results = append(results, entry)
		}

		resp, err := helpers.JSONResult(map[string]any{
			"pipeline":    !transaction,
			"transaction": transaction,
			"count":       len(results),
			"results":     results,
		})
		if err != nil {
			return helpers.ErrorResult("json_error", err.Error()), nil
		}
		return resp, nil
	}
}

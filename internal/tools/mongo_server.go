package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/types/known/structpb"
)

// MongoServerSchema returns the JSON Schema for the mongo_server tool.
func MongoServerSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID for the MongoDB instance",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Server administration action to perform",
				"enum":        []any{"server_status", "db_stats", "list_databases", "current_op", "build_info"},
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// MongoServer returns a tool handler for MongoDB server administration operations.
func MongoServer(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		action := helpers.GetString(req.Arguments, "action")

		if err := helpers.ValidateOneOf(action, "server_status", "db_stats", "list_databases", "current_op", "build_info"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		mp, errResp := getMongoDBProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		switch action {
		case "server_status":
			var result bson.M
			if err := mp.Database().RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&result); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			out := map[string]any{
				"host":    fmt.Sprintf("%v", result["host"]),
				"version": fmt.Sprintf("%v", result["version"]),
				"uptime":  fmt.Sprintf("%v", result["uptime"]),
			}

			if connections, ok := result["connections"].(bson.M); ok {
				out["connections"] = map[string]any{
					"current":   fmt.Sprintf("%v", connections["current"]),
					"available": fmt.Sprintf("%v", connections["available"]),
				}
			}

			if opcounters, ok := result["opcounters"].(bson.M); ok {
				out["opcounters"] = convertBsonM(opcounters)
			}

			if mem, ok := result["mem"].(bson.M); ok {
				out["mem"] = convertBsonM(mem)
			}

			return helpers.JSONResult(out)

		case "db_stats":
			var result bson.M
			if err := mp.Database().RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&result); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			out := make(map[string]any, len(result))
			for k, v := range result {
				out[k] = fmt.Sprintf("%v", v)
			}

			return helpers.JSONResult(out)

		case "list_databases":
			var result bson.M
			if err := mp.Client().Database("admin").RunCommand(ctx, bson.D{{Key: "listDatabases", Value: 1}}).Decode(&result); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			var databases []map[string]any
			if dbs, ok := result["databases"].(bson.A); ok {
				for _, item := range dbs {
					if dbDoc, ok := item.(bson.M); ok {
						databases = append(databases, convertBsonM(dbDoc))
					}
				}
			}

			if databases == nil {
				databases = []map[string]any{}
			}

			return helpers.JSONResult(map[string]any{
				"databases": databases,
				"totalSize": fmt.Sprintf("%v", result["totalSize"]),
			})

		case "current_op":
			var result bson.M
			if err := mp.Client().Database("admin").RunCommand(ctx, bson.D{{Key: "currentOp", Value: 1}}).Decode(&result); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			var operations []map[string]any
			if inprog, ok := result["inprog"].(bson.A); ok {
				for _, item := range inprog {
					if opDoc, ok := item.(bson.M); ok {
						operations = append(operations, convertBsonM(opDoc))
					}
				}
			}

			if operations == nil {
				operations = []map[string]any{}
			}

			return helpers.JSONResult(map[string]any{
				"operations": operations,
				"count":      len(operations),
			})

		case "build_info":
			var result bson.M
			if err := mp.Database().RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&result); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"version":          fmt.Sprintf("%v", result["version"]),
				"gitVersion":       fmt.Sprintf("%v", result["gitVersion"]),
				"modules":          fmt.Sprintf("%v", result["modules"]),
				"allocator":        fmt.Sprintf("%v", result["allocator"]),
				"javascriptEngine": fmt.Sprintf("%v", result["javascriptEngine"]),
				"sysInfo":          fmt.Sprintf("%v", result["sysInfo"]),
			})

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

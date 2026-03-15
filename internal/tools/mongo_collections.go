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

// MongoCollectionsSchema returns the JSON Schema for the mongo_collections tool.
func MongoCollectionsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID for the MongoDB instance",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Collection operation to perform",
				"enum":        []any{"list", "create", "drop", "rename", "stats"},
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Collection name",
			},
			"new_name": map[string]any{
				"type":        "string",
				"description": "New name for rename operation",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// MongoCollections returns a tool handler for MongoDB collection management operations.
func MongoCollections(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		action := helpers.GetString(req.Arguments, "action")

		if err := helpers.ValidateOneOf(action, "list", "create", "drop", "rename", "stats"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		mp, errResp := getMongoDBProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		switch action {
		case "list":
			collections, err := mp.Database().ListCollectionNames(ctx, bson.M{})
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"count":       len(collections),
				"collections": collections,
			})

		case "create":
			name := helpers.GetString(req.Arguments, "name")
			if name == "" {
				return helpers.ErrorResult("validation_error", "name is required for create"), nil
			}

			if err := mp.Database().CreateCollection(ctx, name); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.TextResult(fmt.Sprintf("Created collection '%s'", name)), nil

		case "drop":
			name := helpers.GetString(req.Arguments, "name")
			if name == "" {
				return helpers.ErrorResult("validation_error", "name is required for drop"), nil
			}

			if err := mp.Database().Collection(name).Drop(ctx); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.TextResult(fmt.Sprintf("Dropped collection '%s'", name)), nil

		case "rename":
			name := helpers.GetString(req.Arguments, "name")
			if name == "" {
				return helpers.ErrorResult("validation_error", "name is required for rename"), nil
			}
			newName := helpers.GetString(req.Arguments, "new_name")
			if newName == "" {
				return helpers.ErrorResult("validation_error", "new_name is required for rename"), nil
			}

			// renameCollection must be run on the admin database.
			cmd := bson.D{
				{Key: "renameCollection", Value: mp.DBName() + "." + name},
				{Key: "to", Value: mp.DBName() + "." + newName},
			}

			if err := mp.Client().Database("admin").RunCommand(ctx, cmd).Err(); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.TextResult(fmt.Sprintf("Renamed collection '%s' to '%s'", name, newName)), nil

		case "stats":
			name := helpers.GetString(req.Arguments, "name")
			if name == "" {
				return helpers.ErrorResult("validation_error", "name is required for stats"), nil
			}

			cmd := bson.D{{Key: "collStats", Value: name}}

			var result bson.M
			if err := mp.Database().RunCommand(ctx, cmd).Decode(&result); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"count":          result["count"],
				"size":           result["size"],
				"avgObjSize":     result["avgObjSize"],
				"storageSize":    result["storageSize"],
				"totalIndexSize": result["totalIndexSize"],
				"nindexes":       result["nindexes"],
			})

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

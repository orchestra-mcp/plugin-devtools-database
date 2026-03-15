package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/types/known/structpb"
)

// MongoIndexesSchema returns the JSON Schema for the mongo_indexes tool.
func MongoIndexesSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID for the MongoDB instance",
			},
			"collection": map[string]any{
				"type":        "string",
				"description": "Collection name",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Index operation to perform",
				"enum":        []any{"list", "create", "create_text", "create_ttl", "drop"},
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Index name (for create/drop)",
			},
			"keys": map[string]any{
				"type":        "string",
				"description": `JSON object specifying index keys, e.g. {"email": 1} or {"location": "2dsphere"}`,
			},
			"unique": map[string]any{
				"type":        "boolean",
				"description": "Create a unique index",
			},
			"field": map[string]any{
				"type":        "string",
				"description": "Field name for text/TTL index",
			},
			"expire_after_seconds": map[string]any{
				"type":        "integer",
				"description": "TTL in seconds for TTL index",
			},
		},
		"required": []any{"connection_id", "collection", "action"},
	})
	return s
}

// MongoIndexes returns a tool handler for MongoDB index management operations.
func MongoIndexes(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "collection", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		collection := helpers.GetString(req.Arguments, "collection")
		action := helpers.GetString(req.Arguments, "action")

		if err := helpers.ValidateOneOf(action, "list", "create", "create_text", "create_ttl", "drop"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		mp, errResp := getMongoDBProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		coll := mp.Database().Collection(collection)

		switch action {
		case "list":
			cursor, err := coll.Indexes().List(ctx)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}
			defer cursor.Close(ctx)

			var indexes []map[string]any
			for cursor.Next(ctx) {
				var idx bson.M
				if err := cursor.Decode(&idx); err != nil {
					continue
				}

				entry := make(map[string]any)
				if name, ok := idx["name"]; ok {
					entry["name"] = fmt.Sprintf("%v", name)
				}
				if key, ok := idx["key"]; ok {
					if keyDoc, ok := key.(bson.M); ok {
						entry["keys"] = convertBsonM(keyDoc)
					}
				}
				if unique, ok := idx["unique"].(bool); ok {
					entry["unique"] = unique
				}

				// Include other useful fields if present.
				for _, field := range []string{"expireAfterSeconds", "sparse", "partialFilterExpression", "textIndexVersion", "default_language", "weights"} {
					if v, ok := idx[field]; ok {
						entry[field] = fmt.Sprintf("%v", v)
					}
				}

				indexes = append(indexes, entry)
			}

			if indexes == nil {
				indexes = []map[string]any{}
			}

			return helpers.JSONResult(map[string]any{
				"count":   len(indexes),
				"indexes": indexes,
			})

		case "create":
			keysStr := helpers.GetString(req.Arguments, "keys")
			if keysStr == "" {
				return helpers.ErrorResult("validation_error", "keys is required for create"), nil
			}

			var keys bson.D
			if err := bson.UnmarshalExtJSON([]byte(keysStr), true, &keys); err != nil {
				return helpers.ErrorResult("parse_error", fmt.Sprintf("invalid keys JSON: %s", err.Error())), nil
			}

			opts := options.Index()
			if name := helpers.GetString(req.Arguments, "name"); name != "" {
				opts.SetName(name)
			}
			if helpers.GetBool(req.Arguments, "unique") {
				opts.SetUnique(true)
			}

			model := mongo.IndexModel{
				Keys:    keys,
				Options: opts,
			}

			createdName, err := coll.Indexes().CreateOne(ctx, model)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"created_index": createdName,
			})

		case "create_text":
			field := helpers.GetString(req.Arguments, "field")
			if field == "" {
				return helpers.ErrorResult("validation_error", "field is required for create_text"), nil
			}

			keys := bson.D{{Key: field, Value: "text"}}

			opts := options.Index()
			if name := helpers.GetString(req.Arguments, "name"); name != "" {
				opts.SetName(name)
			}

			model := mongo.IndexModel{
				Keys:    keys,
				Options: opts,
			}

			createdName, err := coll.Indexes().CreateOne(ctx, model)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"created_index": createdName,
			})

		case "create_ttl":
			field := helpers.GetString(req.Arguments, "field")
			if field == "" {
				return helpers.ErrorResult("validation_error", "field is required for create_ttl"), nil
			}

			seconds := helpers.GetInt(req.Arguments, "expire_after_seconds")
			if seconds <= 0 {
				return helpers.ErrorResult("validation_error", "expire_after_seconds must be a positive integer for create_ttl"), nil
			}

			keys := bson.D{{Key: field, Value: 1}}

			opts := options.Index().SetExpireAfterSeconds(int32(seconds))
			if name := helpers.GetString(req.Arguments, "name"); name != "" {
				opts.SetName(name)
			}

			model := mongo.IndexModel{
				Keys:    keys,
				Options: opts,
			}

			createdName, err := coll.Indexes().CreateOne(ctx, model)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"created_index": createdName,
			})

		case "drop":
			name := helpers.GetString(req.Arguments, "name")
			if name == "" {
				return helpers.ErrorResult("validation_error", "name is required for drop"), nil
			}

			if err := coll.Indexes().DropOne(ctx, name); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.TextResult(fmt.Sprintf("Index '%s' dropped from collection '%s'", name, collection)), nil

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

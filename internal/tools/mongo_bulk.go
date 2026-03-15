package tools

import (
	"context"
	"encoding/json"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/types/known/structpb"
)

// MongoBulkSchema returns the JSON Schema for the mongo_bulk tool.
func MongoBulkSchema() *structpb.Struct {
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
			"operations": map[string]any{
				"type":        "string",
				"description": "JSON array of operations. Each operation is an object with a 'type' field (insert, update, delete) and corresponding fields: 'document' for insert, 'filter'+'update' for update, 'filter' for delete.",
			},
			"ordered": map[string]any{
				"type":        "boolean",
				"description": "If true (default), stop on first error. If false, continue on errors.",
			},
		},
		"required": []any{"connection_id", "collection", "operations"},
	})
	return s
}

// MongoBulk returns a tool handler for MongoDB bulk write operations.
func MongoBulk(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "collection", "operations"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		collection := helpers.GetString(req.Arguments, "collection")
		operationsStr := helpers.GetString(req.Arguments, "operations")

		mp, errResp := getMongoDBProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		// Parse operations JSON string.
		var ops []map[string]any
		if err := json.Unmarshal([]byte(operationsStr), &ops); err != nil {
			return helpers.ErrorResult("parse_error", fmt.Sprintf("invalid operations JSON: %s", err.Error())), nil
		}
		if len(ops) == 0 {
			return helpers.ErrorResult("validation_error", "operations array must not be empty"), nil
		}

		// Build write models from operations.
		models := make([]mongo.WriteModel, 0, len(ops))
		for i, op := range ops {
			opType, _ := op["type"].(string)
			switch opType {
			case "insert":
				doc, ok := op["document"].(map[string]any)
				if !ok {
					return helpers.ErrorResult("parse_error", fmt.Sprintf("operation %d: 'document' is required for insert", i)), nil
				}
				models = append(models, mongo.NewInsertOneModel().SetDocument(toBsonM(doc)))

			case "update":
				filter, ok := op["filter"].(map[string]any)
				if !ok {
					return helpers.ErrorResult("parse_error", fmt.Sprintf("operation %d: 'filter' is required for update", i)), nil
				}
				update, ok := op["update"].(map[string]any)
				if !ok {
					return helpers.ErrorResult("parse_error", fmt.Sprintf("operation %d: 'update' is required for update", i)), nil
				}
				models = append(models, mongo.NewUpdateOneModel().SetFilter(toBsonM(filter)).SetUpdate(toBsonM(update)))

			case "delete":
				filter, ok := op["filter"].(map[string]any)
				if !ok {
					return helpers.ErrorResult("parse_error", fmt.Sprintf("operation %d: 'filter' is required for delete", i)), nil
				}
				models = append(models, mongo.NewDeleteOneModel().SetFilter(toBsonM(filter)))

			default:
				return helpers.ErrorResult("validation_error", fmt.Sprintf("operation %d: unknown type %q (must be insert, update, or delete)", i, opType)), nil
			}
		}

		// Build bulk write options.
		opts := options.BulkWrite()
		// Default is ordered=true; only set if explicitly provided as false.
		if req.Arguments != nil {
			if v, exists := req.Arguments.Fields["ordered"]; exists {
				opts.SetOrdered(helpers.GetBool(req.Arguments, "ordered"))
				_ = v // existence check only
			}
		}

		result, err := mp.Database().Collection(collection).BulkWrite(ctx, models, opts)
		if err != nil {
			return helpers.ErrorResult("mongodb_error", err.Error()), nil
		}

		return helpers.JSONResult(map[string]any{
			"inserted_count": result.InsertedCount,
			"modified_count": result.ModifiedCount,
			"deleted_count":  result.DeletedCount,
			"upserted_count": result.UpsertedCount,
		})
	}
}

// toBsonM converts a map[string]any to bson.M, recursively converting nested maps.
func toBsonM(m map[string]any) bson.M {
	result := make(bson.M, len(m))
	for k, v := range m {
		switch val := v.(type) {
		case map[string]any:
			result[k] = toBsonM(val)
		case []any:
			arr := make(bson.A, len(val))
			for i, item := range val {
				if nested, ok := item.(map[string]any); ok {
					arr[i] = toBsonM(nested)
				} else {
					arr[i] = item
				}
			}
			result[k] = arr
		default:
			result[k] = v
		}
	}
	return result
}

package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/types/known/structpb"
)

// MongoSchemaSchema returns the JSON Schema for the mongo_schema tool.
func MongoSchemaSchema() *structpb.Struct {
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
				"description": "Schema operation to perform",
				"enum":        []any{"sample", "validate"},
			},
			"sample_size": map[string]any{
				"type":        "integer",
				"description": "Number of documents to sample for schema inference (default 10)",
			},
		},
		"required": []any{"connection_id", "collection", "action"},
	})
	return s
}

// MongoSchema returns a tool handler for MongoDB schema analysis operations.
func MongoSchema(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "collection", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		collection := helpers.GetString(req.Arguments, "collection")
		action := helpers.GetString(req.Arguments, "action")

		if err := helpers.ValidateOneOf(action, "sample", "validate"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		mp, errResp := getMongoDBProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		coll := mp.Database().Collection(collection)

		switch action {
		case "sample":
			sampleSize := helpers.GetInt(req.Arguments, "sample_size")
			if sampleSize == 0 {
				sampleSize = 10
			}

			opts := options.Find().SetLimit(int64(sampleSize))
			cursor, err := coll.Find(ctx, bson.M{}, opts)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}
			defer cursor.Close(ctx)

			var docs []bson.M
			if err := cursor.All(ctx, &docs); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			// Track field types across all sampled documents.
			type fieldInfo struct {
				types    map[string]int
				count    int
				nullable bool
			}
			fields := make(map[string]*fieldInfo)

			for _, doc := range docs {
				seen := make(map[string]bool)
				for k, v := range doc {
					seen[k] = true
					fi, ok := fields[k]
					if !ok {
						fi = &fieldInfo{types: make(map[string]int)}
						fields[k] = fi
					}
					fi.count++
					typeName := fmt.Sprintf("%T", v)
					fi.types[typeName]++
				}
				// Fields not present in this document are nullable.
				for k, fi := range fields {
					if !seen[k] {
						fi.nullable = true
					}
				}
			}

			// Build schema map with most common type per field.
			schemaMap := make(map[string]any, len(fields))
			for name, fi := range fields {
				// Find the most common type.
				var mostCommon string
				var maxCount int
				for t, c := range fi.types {
					if c > maxCount {
						mostCommon = t
						maxCount = c
					}
				}

				// If a field doesn't appear in every document, it's nullable.
				if fi.count < len(docs) {
					fi.nullable = true
				}

				schemaMap[name] = map[string]any{
					"type":     mostCommon,
					"count":    fi.count,
					"nullable": fi.nullable,
				}
			}

			return helpers.JSONResult(map[string]any{
				"collection":  collection,
				"sample_size": len(docs),
				"fields":      schemaMap,
			})

		case "validate":
			cmd := bson.D{{Key: "validate", Value: collection}}

			var result bson.M
			if err := mp.Database().RunCommand(ctx, cmd).Decode(&result); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"valid":        result["valid"],
				"errors":       result["errors"],
				"nrecords":     result["nrecords"],
				"nIndexes":     result["nIndexes"],
				"keysPerIndex": result["keysPerIndex"],
			})

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

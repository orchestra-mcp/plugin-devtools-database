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

// MongoAggregateSchema returns the JSON Schema for the mongo_aggregate tool.
func MongoAggregateSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID for the MongoDB instance",
			},
			"collection": map[string]any{
				"type":        "string",
				"description": "Collection name to aggregate",
			},
			"pipeline": map[string]any{
				"type":        "string",
				"description": `JSON array of aggregation pipeline stages, e.g. [{"$match": {"status": "active"}}, {"$group": {"_id": "$category", "count": {"$sum": 1}}}]`,
			},
			"allow_disk_use": map[string]any{
				"type":        "boolean",
				"description": "Allow writing temporary files to disk for large aggregations",
			},
		},
		"required": []any{"connection_id", "collection", "pipeline"},
	})
	return s
}

// MongoAggregate returns a tool handler for MongoDB aggregation pipeline operations.
func MongoAggregate(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "collection", "pipeline"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		collection := helpers.GetString(req.Arguments, "collection")
		pipelineStr := helpers.GetString(req.Arguments, "pipeline")

		mp, errResp := getMongoDBProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		// Parse the pipeline JSON string into a bson.A.
		var pipeline bson.A
		if err := bson.UnmarshalExtJSON([]byte(pipelineStr), true, &pipeline); err != nil {
			return helpers.ErrorResult("parse_error", fmt.Sprintf("invalid pipeline JSON: %s", err.Error())), nil
		}

		// Build aggregation options.
		opts := options.Aggregate()
		if helpers.GetBool(req.Arguments, "allow_disk_use") {
			opts.SetAllowDiskUse(true)
		}

		cursor, err := mp.Database().Collection(collection).Aggregate(ctx, pipeline, opts)
		if err != nil {
			return helpers.ErrorResult("mongodb_error", err.Error()), nil
		}
		defer cursor.Close(ctx)

		var results []bson.M
		if err := cursor.All(ctx, &results); err != nil {
			return helpers.ErrorResult("mongodb_error", err.Error()), nil
		}

		docs := make([]map[string]any, len(results))
		for i, doc := range results {
			docs[i] = convertBsonM(doc)
		}

		return helpers.JSONResult(map[string]any{
			"count":   len(docs),
			"results": docs,
		})
	}
}

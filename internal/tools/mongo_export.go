package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/types/known/structpb"
)

// MongoExportSchema returns the JSON Schema for the mongo_export tool.
func MongoExportSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID for the MongoDB instance",
			},
			"collection": map[string]any{
				"type":        "string",
				"description": "Collection to export",
			},
			"path": map[string]any{
				"type":        "string",
				"description": "Output file path (defaults to ./<collection>.json)",
			},
			"filter": map[string]any{
				"type":        "string",
				"description": "JSON filter to export a subset of documents",
			},
		},
		"required": []any{"connection_id", "collection"},
	})
	return s
}

// MongoExport returns a tool handler that exports a MongoDB collection to a JSON file.
func MongoExport(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "collection"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		collection := helpers.GetString(req.Arguments, "collection")

		mp, errResp := getMongoDBProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		// Parse filter (default to empty filter for all documents).
		filter := bson.M{}
		if filterStr := helpers.GetString(req.Arguments, "filter"); filterStr != "" {
			if err := bson.UnmarshalExtJSON([]byte(filterStr), true, &filter); err != nil {
				return helpers.ErrorResult("parse_error", fmt.Sprintf("invalid filter JSON: %s", err.Error())), nil
			}
		}

		// Determine output path.
		path := helpers.GetString(req.Arguments, "path")
		if path == "" {
			path = collection + ".json"
		}

		// Ensure parent directory exists.
		if dir := filepath.Dir(path); dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return helpers.ErrorResult("filesystem_error", fmt.Sprintf("failed to create directory %s: %s", dir, err.Error())), nil
			}
		}

		// Find all matching documents.
		cursor, err := mp.Database().Collection(collection).Find(ctx, filter)
		if err != nil {
			return helpers.ErrorResult("mongodb_error", err.Error()), nil
		}
		defer cursor.Close(ctx)

		var results []bson.M
		if err := cursor.All(ctx, &results); err != nil {
			return helpers.ErrorResult("mongodb_error", err.Error()), nil
		}

		// Convert bson.M documents to plain map[string]any for clean JSON output.
		docs := make([]map[string]any, len(results))
		for i, doc := range results {
			docs[i] = convertBsonM(doc)
		}

		// Marshal to indented JSON.
		data, err := json.MarshalIndent(docs, "", "  ")
		if err != nil {
			return helpers.ErrorResult("marshal_error", fmt.Sprintf("failed to marshal documents: %s", err.Error())), nil
		}

		// Write to file.
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return helpers.ErrorResult("filesystem_error", fmt.Sprintf("failed to write file %s: %s", path, err.Error())), nil
		}

		return helpers.TextResult(fmt.Sprintf("Exported %d documents from %s to %s.", len(docs), collection, path)), nil
	}
}

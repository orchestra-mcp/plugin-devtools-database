package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// MongoImportSchema returns the JSON Schema for the mongo_import tool.
func MongoImportSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID for the MongoDB instance",
			},
			"collection": map[string]any{
				"type":        "string",
				"description": "Target collection name",
			},
			"path": map[string]any{
				"type":        "string",
				"description": "Path to JSON file to import (must contain a JSON array of objects)",
			},
			"drop_first": map[string]any{
				"type":        "boolean",
				"description": "Drop the collection before importing",
			},
		},
		"required": []any{"connection_id", "collection", "path"},
	})
	return s
}

// MongoImport returns a tool handler that imports documents from a JSON file into a MongoDB collection.
func MongoImport(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "collection", "path"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		collection := helpers.GetString(req.Arguments, "collection")
		path := helpers.GetString(req.Arguments, "path")

		mp, errResp := getMongoDBProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		// Read the JSON file.
		data, err := os.ReadFile(path)
		if err != nil {
			return helpers.ErrorResult("filesystem_error", fmt.Sprintf("failed to read file %s: %s", path, err.Error())), nil
		}

		// Parse the JSON array of objects.
		var rows []map[string]any
		if err := json.Unmarshal(data, &rows); err != nil {
			return helpers.ErrorResult("parse_error", fmt.Sprintf("invalid JSON (expected array of objects): %s", err.Error())), nil
		}

		if len(rows) == 0 {
			return helpers.TextResult("File contains no documents to import."), nil
		}

		coll := mp.Database().Collection(collection)

		// Optionally drop the collection before importing.
		if helpers.GetBool(req.Arguments, "drop_first") {
			if err := coll.Drop(ctx); err != nil {
				return helpers.ErrorResult("mongodb_error", fmt.Sprintf("failed to drop collection %s: %s", collection, err.Error())), nil
			}
		}

		// Convert each map to bson.M and build the documents slice.
		docs := make([]any, len(rows))
		for i, row := range rows {
			docs[i] = toBsonM(row)
		}

		// Insert all documents.
		_, err = coll.InsertMany(ctx, docs)
		if err != nil {
			return helpers.ErrorResult("mongodb_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Imported %d documents into %s from %s.", len(docs), collection, path)), nil
	}
}

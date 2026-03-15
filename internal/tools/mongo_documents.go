package tools

import (
	"context"
	"encoding/json"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/types/known/structpb"
)

// MongoDocumentsSchema returns the JSON Schema for the mongo_documents tool.
func MongoDocumentsSchema() *structpb.Struct {
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
				"description": "Document operation to perform",
				"enum":        []any{"find", "find_one", "insert_one", "insert_many", "update_one", "update_many", "delete_one", "delete_many", "count", "distinct"},
			},
			"filter": map[string]any{
				"type":        "string",
				"description": "JSON filter document, e.g. {\"name\": \"John\"}",
			},
			"document": map[string]any{
				"type":        "string",
				"description": "JSON document for insert operations",
			},
			"documents": map[string]any{
				"type":        "string",
				"description": "JSON array of documents for insert_many",
			},
			"update": map[string]any{
				"type":        "string",
				"description": "JSON update document, e.g. {\"$set\": {\"name\": \"Jane\"}}",
			},
			"field": map[string]any{
				"type":        "string",
				"description": "Field name for distinct operation",
			},
			"sort": map[string]any{
				"type":        "string",
				"description": "JSON sort specification, e.g. {\"created_at\": -1}",
			},
			"limit": map[string]any{
				"type":        "integer",
				"description": "Maximum number of documents to return",
			},
			"skip": map[string]any{
				"type":        "integer",
				"description": "Number of documents to skip",
			},
		},
		"required": []any{"connection_id", "collection", "action"},
	})
	return s
}

// MongoDocuments returns a tool handler for MongoDB document CRUD operations.
func MongoDocuments(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "collection", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		collection := helpers.GetString(req.Arguments, "collection")
		action := helpers.GetString(req.Arguments, "action")

		if err := helpers.ValidateOneOf(action, "find", "find_one", "insert_one", "insert_many", "update_one", "update_many", "delete_one", "delete_many", "count", "distinct"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		mp, errResp := getMongoDBProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		coll := mp.Database().Collection(collection)

		switch action {
		case "find":
			filter, err := parseFilter(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}

			opts := options.Find()
			if sortStr := helpers.GetString(req.Arguments, "sort"); sortStr != "" {
				var sortDoc bson.M
				if err := bson.UnmarshalExtJSON([]byte(sortStr), true, &sortDoc); err != nil {
					return helpers.ErrorResult("parse_error", fmt.Sprintf("invalid sort JSON: %s", err.Error())), nil
				}
				opts.SetSort(sortDoc)
			}
			if limit := helpers.GetInt(req.Arguments, "limit"); limit > 0 {
				opts.SetLimit(int64(limit))
			}
			if skip := helpers.GetInt(req.Arguments, "skip"); skip > 0 {
				opts.SetSkip(int64(skip))
			}

			cursor, err := coll.Find(ctx, filter, opts)
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
				"count":     len(docs),
				"documents": docs,
			})

		case "find_one":
			filter, err := parseFilter(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}

			opts := options.FindOne()
			if sortStr := helpers.GetString(req.Arguments, "sort"); sortStr != "" {
				var sortDoc bson.M
				if err := bson.UnmarshalExtJSON([]byte(sortStr), true, &sortDoc); err != nil {
					return helpers.ErrorResult("parse_error", fmt.Sprintf("invalid sort JSON: %s", err.Error())), nil
				}
				opts.SetSort(sortDoc)
			}

			var doc bson.M
			if err := coll.FindOne(ctx, filter, opts).Decode(&doc); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"document": convertBsonM(doc),
			})

		case "insert_one":
			docStr := helpers.GetString(req.Arguments, "document")
			if docStr == "" {
				return helpers.ErrorResult("validation_error", "document is required for insert_one"), nil
			}
			var doc bson.M
			if err := bson.UnmarshalExtJSON([]byte(docStr), true, &doc); err != nil {
				return helpers.ErrorResult("parse_error", fmt.Sprintf("invalid document JSON: %s", err.Error())), nil
			}

			result, err := coll.InsertOne(ctx, doc)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"inserted_id": fmt.Sprintf("%v", result.InsertedID),
			})

		case "insert_many":
			docsStr := helpers.GetString(req.Arguments, "documents")
			if docsStr == "" {
				return helpers.ErrorResult("validation_error", "documents is required for insert_many"), nil
			}
			var docs []bson.M
			if err := json.Unmarshal([]byte(docsStr), &docs); err != nil {
				return helpers.ErrorResult("parse_error", fmt.Sprintf("invalid documents JSON array: %s", err.Error())), nil
			}
			if len(docs) == 0 {
				return helpers.ErrorResult("validation_error", "documents array must not be empty"), nil
			}

			// Convert []bson.M to []any for InsertMany.
			bsonDocs := make([]any, len(docs))
			for i, d := range docs {
				bsonDocs[i] = d
			}

			result, err := coll.InsertMany(ctx, bsonDocs)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			ids := make([]string, len(result.InsertedIDs))
			for i, id := range result.InsertedIDs {
				ids[i] = fmt.Sprintf("%v", id)
			}

			return helpers.JSONResult(map[string]any{
				"inserted_count": len(result.InsertedIDs),
				"inserted_ids":   ids,
			})

		case "update_one":
			filter, err := parseFilter(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}
			update, err := parseUpdate(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}

			result, err := coll.UpdateOne(ctx, filter, update)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"matched_count":  result.MatchedCount,
				"modified_count": result.ModifiedCount,
			})

		case "update_many":
			filter, err := parseFilter(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}
			update, err := parseUpdate(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}

			result, err := coll.UpdateMany(ctx, filter, update)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"matched_count":  result.MatchedCount,
				"modified_count": result.ModifiedCount,
			})

		case "delete_one":
			filter, err := parseFilter(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}

			result, err := coll.DeleteOne(ctx, filter)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"deleted_count": result.DeletedCount,
			})

		case "delete_many":
			filter, err := parseFilter(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}

			result, err := coll.DeleteMany(ctx, filter)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"deleted_count": result.DeletedCount,
			})

		case "count":
			filter, err := parseFilter(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}

			count, err := coll.CountDocuments(ctx, filter)
			if err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			return helpers.JSONResult(map[string]any{
				"count": count,
			})

		case "distinct":
			field := helpers.GetString(req.Arguments, "field")
			if field == "" {
				return helpers.ErrorResult("validation_error", "field is required for distinct"), nil
			}

			filter, err := parseFilter(req)
			if err != nil {
				return helpers.ErrorResult("parse_error", err.Error()), nil
			}

			distinctResult := coll.Distinct(ctx, field, filter)
			if err := distinctResult.Err(); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			var values []any
			if err := distinctResult.Decode(&values); err != nil {
				return helpers.ErrorResult("mongodb_error", err.Error()), nil
			}

			strValues := make([]any, len(values))
			for i, v := range values {
				strValues[i] = fmt.Sprintf("%v", v)
			}

			return helpers.JSONResult(map[string]any{
				"values": strValues,
				"count":  len(values),
			})

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action: %s", action)), nil
		}
	}
}

// parseFilter extracts and parses the filter JSON from the request.
// Returns an empty filter if none is provided.
func parseFilter(req *pluginv1.ToolRequest) (bson.M, error) {
	filterStr := helpers.GetString(req.Arguments, "filter")
	if filterStr == "" {
		return bson.M{}, nil
	}
	var filter bson.M
	if err := bson.UnmarshalExtJSON([]byte(filterStr), true, &filter); err != nil {
		return nil, fmt.Errorf("invalid filter JSON: %w", err)
	}
	return filter, nil
}

// parseUpdate extracts and parses the update JSON from the request.
func parseUpdate(req *pluginv1.ToolRequest) (bson.M, error) {
	updateStr := helpers.GetString(req.Arguments, "update")
	if updateStr == "" {
		return nil, fmt.Errorf("update is required for update operations")
	}
	var update bson.M
	if err := bson.UnmarshalExtJSON([]byte(updateStr), true, &update); err != nil {
		return nil, fmt.Errorf("invalid update JSON: %w", err)
	}
	return update, nil
}

// convertBsonM converts a bson.M document to map[string]any with string-formatted values.
func convertBsonM(doc bson.M) map[string]any {
	result := make(map[string]any, len(doc))
	for k, v := range doc {
		switch val := v.(type) {
		case bson.M:
			result[k] = convertBsonM(val)
		case bson.A:
			arr := make([]any, len(val))
			for i, item := range val {
				if m, ok := item.(bson.M); ok {
					arr[i] = convertBsonM(m)
				} else {
					arr[i] = fmt.Sprintf("%v", item)
				}
			}
			result[k] = arr
		default:
			result[k] = fmt.Sprintf("%v", val)
		}
	}
	return result
}

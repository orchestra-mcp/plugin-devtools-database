package tools

import (
	"context"
	"errors"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbCreateTableSchema returns the JSON Schema for the db_create_table tool.
func DbCreateTableSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Table name",
			},
			"columns": map[string]any{
				"type":        "array",
				"description": "Column definitions",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"name": map[string]any{"type": "string"},
						"type": map[string]any{
							"type":        "string",
							"description": "Canonical type: string, text, integer, bigint, float, decimal, boolean, timestamp, date, json, blob, uuid, serial",
						},
						"nullable":       map[string]any{"type": "boolean"},
						"default":        map[string]any{"type": "string"},
						"primary_key":    map[string]any{"type": "boolean"},
						"auto_increment": map[string]any{"type": "boolean"},
						"unique":         map[string]any{"type": "boolean"},
						"references": map[string]any{
							"type":        "string",
							"description": "Foreign key: table(column)",
						},
					},
					"required": []any{"name", "type"},
				},
			},
			"if_not_exists": map[string]any{
				"type":        "boolean",
				"description": "Add IF NOT EXISTS clause",
			},
		},
		"required": []any{"connection_id", "name", "columns"},
	})
	return s
}

// DbCreateTable returns a tool handler that creates a new table.
func DbCreateTable(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		ifNotExists := helpers.GetBool(req.Arguments, "if_not_exists")

		columns, err := parseColumnDefs(req.Arguments, "columns")
		if err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		if len(columns) == 0 {
			return helpers.ErrorResult("validation_error", "at least one column is required"), nil
		}

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		if err := provider.CreateTable(ctx, name, columns, ifNotExists); err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("The %s provider does not support CREATE TABLE.", provider.Kind())), nil
			}
			return helpers.ErrorResult("create_table_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Created table %q with %d column(s).", name, len(columns))), nil
	}
}

// parseColumnDefs extracts a slice of db.ColumnDef from a structpb array field.
func parseColumnDefs(args *structpb.Struct, key string) ([]db.ColumnDef, error) {
	val, ok := args.Fields[key]
	if !ok {
		return nil, fmt.Errorf("missing %q", key)
	}
	list := val.GetListValue()
	if list == nil {
		return nil, fmt.Errorf("%q must be an array", key)
	}
	var cols []db.ColumnDef
	for i, item := range list.Values {
		obj := item.GetStructValue()
		if obj == nil {
			return nil, fmt.Errorf("column %d must be an object", i)
		}
		col := db.ColumnDef{
			Name: getStr(obj, "name"),
			Type: getStr(obj, "type"),
		}
		if v, ok := obj.Fields["nullable"]; ok {
			col.Nullable = v.GetBoolValue()
		}
		if v, ok := obj.Fields["default"]; ok {
			col.Default = v.GetStringValue()
		}
		if v, ok := obj.Fields["primary_key"]; ok {
			col.PrimaryKey = v.GetBoolValue()
		}
		if v, ok := obj.Fields["auto_increment"]; ok {
			col.AutoIncrement = v.GetBoolValue()
		}
		if v, ok := obj.Fields["unique"]; ok {
			col.Unique = v.GetBoolValue()
		}
		if v, ok := obj.Fields["references"]; ok {
			col.References = v.GetStringValue()
		}
		if col.Name == "" || col.Type == "" {
			return nil, fmt.Errorf("column %d: name and type are required", i)
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// getStr reads a string field from a structpb.Struct, returning "" if absent.
func getStr(s *structpb.Struct, key string) string {
	if v, ok := s.Fields[key]; ok {
		return v.GetStringValue()
	}
	return ""
}

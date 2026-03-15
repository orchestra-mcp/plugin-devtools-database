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

// DbAlterTableSchema returns the JSON Schema for the db_alter_table tool.
func DbAlterTableSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to alter",
			},
			"action": map[string]any{
				"type":        "string",
				"enum":        []any{"add_column", "drop_column", "rename_column"},
				"description": "Alter action to perform",
			},
			"column": map[string]any{
				"type":        "object",
				"description": "Column definition (for add_column)",
				"properties": map[string]any{
					"name":           map[string]any{"type": "string"},
					"type":           map[string]any{"type": "string"},
					"nullable":       map[string]any{"type": "boolean"},
					"default":        map[string]any{"type": "string"},
					"auto_increment": map[string]any{"type": "boolean"},
					"unique":         map[string]any{"type": "boolean"},
					"references":     map[string]any{"type": "string"},
				},
			},
			"column_name": map[string]any{
				"type":        "string",
				"description": "Column name (for drop_column/rename_column)",
			},
			"new_name": map[string]any{
				"type":        "string",
				"description": "New column name (for rename_column)",
			},
		},
		"required": []any{"connection_id", "table", "action"},
	})
	return s
}

// DbAlterTable returns a tool handler that alters a table (add/drop/rename column).
func DbAlterTable(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		action := helpers.GetString(req.Arguments, "action")

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		switch action {
		case "add_column":
			col, err := parseSingleColumnDef(req.Arguments, "column")
			if err != nil {
				return helpers.ErrorResult("validation_error", err.Error()), nil
			}
			if err := provider.AlterTableAdd(ctx, table, col); err != nil {
				if errors.Is(err, db.ErrUnsupported) {
					return helpers.ErrorResult("unsupported", fmt.Sprintf("The %s provider does not support ALTER TABLE ADD COLUMN.", provider.Kind())), nil
				}
				return helpers.ErrorResult("alter_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Added column %q to table %q.", col.Name, table)), nil

		case "drop_column":
			if err := helpers.ValidateRequired(req.Arguments, "column_name"); err != nil {
				return helpers.ErrorResult("validation_error", "column_name is required for drop_column"), nil
			}
			colName := helpers.GetString(req.Arguments, "column_name")
			if err := provider.AlterTableDrop(ctx, table, colName); err != nil {
				if errors.Is(err, db.ErrUnsupported) {
					return helpers.ErrorResult("unsupported", fmt.Sprintf("The %s provider does not support ALTER TABLE DROP COLUMN.", provider.Kind())), nil
				}
				return helpers.ErrorResult("alter_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Dropped column %q from table %q.", colName, table)), nil

		case "rename_column":
			if err := helpers.ValidateRequired(req.Arguments, "column_name", "new_name"); err != nil {
				return helpers.ErrorResult("validation_error", "column_name and new_name are required for rename_column"), nil
			}
			colName := helpers.GetString(req.Arguments, "column_name")
			newName := helpers.GetString(req.Arguments, "new_name")
			if err := provider.AlterTableRename(ctx, table, colName, newName); err != nil {
				if errors.Is(err, db.ErrUnsupported) {
					return helpers.ErrorResult("unsupported", fmt.Sprintf("The %s provider does not support ALTER TABLE RENAME COLUMN.", provider.Kind())), nil
				}
				return helpers.ErrorResult("alter_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("Renamed column %q to %q in table %q.", colName, newName, table)), nil

		default:
			return helpers.ErrorResult("validation_error", fmt.Sprintf("unknown action %q (expected add_column, drop_column, or rename_column)", action)), nil
		}
	}
}

// parseSingleColumnDef extracts a single db.ColumnDef from a structpb object field.
func parseSingleColumnDef(args *structpb.Struct, key string) (db.ColumnDef, error) {
	val, ok := args.Fields[key]
	if !ok {
		return db.ColumnDef{}, fmt.Errorf("missing %q", key)
	}
	obj := val.GetStructValue()
	if obj == nil {
		return db.ColumnDef{}, fmt.Errorf("%q must be an object", key)
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
		return db.ColumnDef{}, fmt.Errorf("column name and type are required")
	}
	return col, nil
}

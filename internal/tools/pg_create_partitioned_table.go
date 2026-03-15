package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgCreatePartitionedTableSchema returns the JSON Schema for the pg_create_partitioned_table tool.
func PgCreatePartitionedTableSchema() *structpb.Struct {
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
			"partition_by": map[string]any{
				"type":        "string",
				"description": "Partition strategy",
				"enum":        []any{"range", "list", "hash"},
			},
			"partition_key": map[string]any{
				"type":        "string",
				"description": "Column to partition on",
			},
		},
		"required": []any{"connection_id", "name", "columns", "partition_by", "partition_key"},
	})
	return s
}

// PgCreatePartitionedTable returns a tool handler that creates a partitioned table in PostgreSQL.
func PgCreatePartitionedTable(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name", "partition_by", "partition_key"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		partitionBy := helpers.GetString(req.Arguments, "partition_by")
		partitionKey := helpers.GetString(req.Arguments, "partition_key")

		columns, err := parseColumnDefs(req.Arguments, "columns")
		if err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		if len(columns) == 0 {
			return helpers.ErrorResult("validation_error", "at least one column is required"), nil
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.CreatePartitionedTable(ctx, name, columns, partitionBy, partitionKey); err != nil {
			return helpers.ErrorResult("create_partitioned_table_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Created partitioned table %q (partition by %s on %q) with %d column(s).", name, partitionBy, partitionKey, len(columns))), nil
	}
}

package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgCreatePartitionSchema returns the JSON Schema for the pg_create_partition tool.
func PgCreatePartitionSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"parent": map[string]any{
				"type":        "string",
				"description": "Parent partitioned table name",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Partition table name",
			},
			"bound": map[string]any{
				"type":        "string",
				"description": "Partition bound expression, e.g. FOR VALUES FROM ('2024-01-01') TO ('2024-12-31')",
			},
		},
		"required": []any{"connection_id", "parent", "name", "bound"},
	})
	return s
}

// PgCreatePartition returns a tool handler that creates a partition of a parent table.
func PgCreatePartition(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "parent", "name", "bound"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		parent := helpers.GetString(req.Arguments, "parent")
		name := helpers.GetString(req.Arguments, "name")
		bound := helpers.GetString(req.Arguments, "bound")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.CreatePartition(ctx, parent, name, bound); err != nil {
			return helpers.ErrorResult("create_partition_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Created partition %q of table %q.", name, parent)), nil
	}
}

package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgDetachPartitionSchema returns the JSON Schema for the pg_detach_partition tool.
func PgDetachPartitionSchema() *structpb.Struct {
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
			"partition": map[string]any{
				"type":        "string",
				"description": "Partition table name to detach",
			},
			"concurrent": map[string]any{
				"type":        "boolean",
				"description": "Detach concurrently (non-blocking, PostgreSQL 14+)",
			},
		},
		"required": []any{"connection_id", "parent", "partition"},
	})
	return s
}

// PgDetachPartition returns a tool handler that detaches a partition from its parent table.
func PgDetachPartition(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "parent", "partition"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		parent := helpers.GetString(req.Arguments, "parent")
		partition := helpers.GetString(req.Arguments, "partition")
		concurrent := helpers.GetBool(req.Arguments, "concurrent")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.DetachPartition(ctx, parent, partition, concurrent); err != nil {
			return helpers.ErrorResult("detach_partition_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Detached partition %q from table %q.", partition, parent)), nil
	}
}

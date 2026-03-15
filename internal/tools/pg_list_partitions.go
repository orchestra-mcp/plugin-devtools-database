package tools

import (
	"context"
	"fmt"
	"strings"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgListPartitionsSchema returns the JSON Schema for the pg_list_partitions tool.
func PgListPartitionsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Parent partitioned table name",
			},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

// PgListPartitions returns a tool handler that lists partitions of a PostgreSQL table.
func PgListPartitions(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		partitions, err := pg.ListPartitions(ctx, table)
		if err != nil {
			return helpers.ErrorResult("list_partitions_error", err.Error()), nil
		}

		if len(partitions) == 0 {
			return helpers.TextResult(fmt.Sprintf("No partitions found for table %q.", table)), nil
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("## Partitions of: %s\n\n", table))
		sb.WriteString("| Partition | Bound Expression |\n")
		sb.WriteString("|-----------|------------------|\n")
		for _, p := range partitions {
			sb.WriteString(fmt.Sprintf("| %s | %s |\n", p.Name, p.Expression))
		}

		return helpers.TextResult(sb.String()), nil
	}
}

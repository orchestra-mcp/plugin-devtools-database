package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgClusterSchema returns the JSON Schema for the pg_cluster tool.
func PgClusterSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to cluster (omit to re-cluster all previously clustered tables)",
			},
			"index": map[string]any{
				"type":        "string",
				"description": "Index to cluster the table on (required when clustering a table for the first time)",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgCluster returns a tool handler that runs CLUSTER on a PostgreSQL table.
func PgCluster(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		index := helpers.GetString(req.Arguments, "index")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.Cluster(ctx, table, index); err != nil {
			return helpers.ErrorResult("cluster_error", err.Error()), nil
		}

		target := "all tables"
		if table != "" {
			target = fmt.Sprintf("table %s", table)
		}

		return helpers.TextResult(fmt.Sprintf("CLUSTER completed on %s", target)), nil
	}
}

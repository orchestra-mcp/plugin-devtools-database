package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgEnableRLSSchema returns the JSON Schema for the pg_enable_rls tool.
func PgEnableRLSSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to enable RLS on",
			},
			"force": map[string]any{
				"type":        "boolean",
				"description": "Apply FORCE so RLS policies also affect the table owner (default false)",
			},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

// PgEnableRLS returns a tool handler that enables Row-Level Security on a table.
func PgEnableRLS(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		force := helpers.GetBool(req.Arguments, "force")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.EnableRLS(ctx, table, force); err != nil {
			return helpers.ErrorResult("enable_rls_error", err.Error()), nil
		}

		msg := fmt.Sprintf("Row-Level Security enabled on table %q.", table)
		if force {
			msg += " (FORCE applied)"
		}
		return helpers.TextResult(msg), nil
	}
}

// PgDisableRLSSchema returns the JSON Schema for the pg_disable_rls tool.
func PgDisableRLSSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to disable RLS on",
			},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

// PgDisableRLS returns a tool handler that disables Row-Level Security on a table.
func PgDisableRLS(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
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

		if err := pg.DisableRLS(ctx, table); err != nil {
			return helpers.ErrorResult("disable_rls_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Row-Level Security disabled on table %q.", table)), nil
	}
}

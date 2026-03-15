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

// PgListPoliciesSchema returns the JSON Schema for the pg_list_policies tool.
func PgListPoliciesSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Filter policies by table name (omit to list all)",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgListPolicies returns a tool handler that lists Row-Level Security policies.
func PgListPolicies(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		policies, err := pg.ListPolicies(ctx, table)
		if err != nil {
			return helpers.ErrorResult("list_policies_error", err.Error()), nil
		}

		if len(policies) == 0 {
			return helpers.TextResult("No RLS policies found."), nil
		}

		var sb strings.Builder
		sb.WriteString("| Policy | Table | Permissive | Command | Roles | USING | WITH CHECK |\n")
		sb.WriteString("|--------|-------|------------|---------|-------|-------|------------|\n")
		for _, p := range policies {
			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s |\n",
				p.Name, p.Table, p.Permissive, p.Command, p.Roles, p.Using, p.WithCheck))
		}

		return helpers.TextResult(sb.String()), nil
	}
}

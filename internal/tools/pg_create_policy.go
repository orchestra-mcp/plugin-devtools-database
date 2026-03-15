package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db/providers"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// PgCreatePolicySchema returns the JSON Schema for the pg_create_policy tool.
func PgCreatePolicySchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table the policy applies to",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Policy name",
			},
			"permissive": map[string]any{
				"type":        "boolean",
				"description": "PERMISSIVE (true, default) or RESTRICTIVE (false)",
			},
			"command": map[string]any{
				"type":        "string",
				"description": "SQL command the policy applies to: ALL, SELECT, INSERT, UPDATE, DELETE (default ALL)",
			},
			"roles": map[string]any{
				"type":        "array",
				"description": "Roles the policy applies to (default PUBLIC)",
				"items":       map[string]any{"type": "string"},
			},
			"using": map[string]any{
				"type":        "string",
				"description": "USING expression for existing rows",
			},
			"with_check": map[string]any{
				"type":        "string",
				"description": "WITH CHECK expression for new/modified rows",
			},
		},
		"required": []any{"connection_id", "table", "name"},
	})
	return s
}

// PgCreatePolicy returns a tool handler that creates a Row-Level Security policy.
func PgCreatePolicy(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		name := helpers.GetString(req.Arguments, "name")
		permissive := true
		if v, ok := req.Arguments.Fields["permissive"]; ok {
			permissive = v.GetBoolValue()
		}
		command := helpers.GetString(req.Arguments, "command")
		if command == "" {
			command = "ALL"
		}
		using := helpers.GetString(req.Arguments, "using")
		withCheck := helpers.GetString(req.Arguments, "with_check")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		pol := providers.PolicyDef{
			Table:      table,
			Name:       name,
			Permissive: permissive,
			Command:    command,
			Using:      using,
			WithCheck:  withCheck,
		}

		if rolesVal, ok := req.Arguments.Fields["roles"]; ok && rolesVal.GetListValue() != nil {
			for _, v := range rolesVal.GetListValue().Values {
				if s := v.GetStringValue(); s != "" {
					pol.Roles = append(pol.Roles, s)
				}
			}
		}

		if err := pg.CreatePolicy(ctx, pol); err != nil {
			return helpers.ErrorResult("create_policy_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Created RLS policy %q on table %q.", name, table)), nil
	}
}

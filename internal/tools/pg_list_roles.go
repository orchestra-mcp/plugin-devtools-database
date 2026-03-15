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

func PgListRolesSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
		},
		"required": []any{"connection_id"},
	})
	return s
}

func PgListRoles(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		roles, err := pg.ListRoles(ctx)
		if err != nil {
			return helpers.ErrorResult("list_roles_error", err.Error()), nil
		}
		if len(roles) == 0 {
			return helpers.TextResult("No roles found."), nil
		}

		boolStr := func(v bool) string {
			if v {
				return "Yes"
			}
			return "No"
		}

		var b strings.Builder
		b.WriteString("| Role | Login | SuperUser | CreateDB | CreateRole | Replication | ConnLimit |\n")
		b.WriteString("|------|-------|-----------|----------|------------|-------------|----------|\n")
		for _, r := range roles {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %d |\n",
				r.Name, boolStr(r.Login), boolStr(r.SuperUser), boolStr(r.CreateDB),
				boolStr(r.CreateRole), boolStr(r.Replication), r.ConnLimit)
		}
		return helpers.TextResult(b.String()), nil
	}
}

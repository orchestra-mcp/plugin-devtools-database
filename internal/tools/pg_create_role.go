package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgCreateRoleSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"name":          map[string]any{"type": "string", "description": "Role name"},
			"password":      map[string]any{"type": "string", "description": "Login password (optional)"},
			"login":         map[string]any{"type": "boolean", "description": "Allow login (default false)"},
			"createdb":      map[string]any{"type": "boolean", "description": "Allow creating databases (default false)"},
			"superuser":     map[string]any{"type": "boolean", "description": "Grant superuser (default false)"},
		},
		"required": []any{"connection_id", "name"},
	})
	return s
}

func PgCreateRole(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		password := helpers.GetString(req.Arguments, "password")
		login := helpers.GetBool(req.Arguments, "login")
		createdb := helpers.GetBool(req.Arguments, "createdb")
		superuser := helpers.GetBool(req.Arguments, "superuser")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.CreateRole(ctx, name, password, login, createdb, superuser); err != nil {
			return helpers.ErrorResult("create_role_error", err.Error()), nil
		}
		return helpers.TextResult(fmt.Sprintf("Created role %q.", name)), nil
	}
}

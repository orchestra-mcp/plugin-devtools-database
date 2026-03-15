package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgRevokeSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"privileges":    map[string]any{"type": "string", "description": "Privileges to revoke (e.g. SELECT, INSERT, ALL PRIVILEGES)"},
			"on":            map[string]any{"type": "string", "description": "Target object (e.g. TABLE users, ALL TABLES IN SCHEMA public)"},
			"from":          map[string]any{"type": "string", "description": "Role name to revoke from"},
		},
		"required": []any{"connection_id", "privileges", "on", "from"},
	})
	return s
}

func PgRevoke(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "privileges", "on", "from"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		privileges := helpers.GetString(req.Arguments, "privileges")
		on := helpers.GetString(req.Arguments, "on")
		from := helpers.GetString(req.Arguments, "from")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.Revoke(ctx, privileges, on, from); err != nil {
			return helpers.ErrorResult("revoke_error", err.Error()), nil
		}
		return helpers.TextResult(fmt.Sprintf("Revoked %s ON %s FROM %s.", privileges, on, from)), nil
	}
}

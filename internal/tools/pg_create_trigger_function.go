package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

func PgCreateTriggerFunctionSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{"type": "string", "description": "Connection ID"},
			"name":          map[string]any{"type": "string", "description": "Function name"},
			"body":          map[string]any{"type": "string", "description": "PL/pgSQL function body (BEGIN...END block)"},
			"language":      map[string]any{"type": "string", "description": "Language (default plpgsql)"},
			"replace":       map[string]any{"type": "boolean", "description": "Use CREATE OR REPLACE (default true)"},
		},
		"required": []any{"connection_id", "name", "body"},
	})
	return s
}

func PgCreateTriggerFunction(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "name", "body"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		name := helpers.GetString(req.Arguments, "name")
		body := helpers.GetString(req.Arguments, "body")
		language := helpers.GetString(req.Arguments, "language")
		replace := true
		if v, ok := req.Arguments.Fields["replace"]; ok {
			replace = v.GetBoolValue()
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.CreateTriggerFunction(ctx, name, body, language, replace); err != nil {
			return helpers.ErrorResult("create_trigger_function_error", err.Error()), nil
		}
		return helpers.TextResult(fmt.Sprintf("Created trigger function %q.", name)), nil
	}
}

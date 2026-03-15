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

// PgSetSearchPathSchema returns the JSON Schema for the pg_set_search_path tool.
func PgSetSearchPathSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"schemas": map[string]any{
				"type":        "array",
				"description": "Ordered list of schema names for the search path",
				"items": map[string]any{
					"type": "string",
				},
			},
		},
		"required": []any{"connection_id", "schemas"},
	})
	return s
}

// PgSetSearchPath returns a tool handler that sets the PostgreSQL search path.
func PgSetSearchPath(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		var schemas []string
		if list := req.Arguments.Fields["schemas"].GetListValue(); list != nil {
			for _, v := range list.Values {
				schemas = append(schemas, v.GetStringValue())
			}
		}
		if len(schemas) == 0 {
			return helpers.ErrorResult("validation_error", "at least one schema is required"), nil
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		if err := pg.SetSearchPath(ctx, schemas); err != nil {
			return helpers.ErrorResult("schema_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Search path set to: %s", strings.Join(schemas, ", "))), nil
	}
}

// PgGetSearchPathSchema returns the JSON Schema for the pg_get_search_path tool.
func PgGetSearchPathSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgGetSearchPath returns a tool handler that retrieves the current PostgreSQL search path.
func PgGetSearchPath(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		path, err := pg.GetSearchPath(ctx)
		if err != nil {
			return helpers.ErrorResult("schema_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Current search path: %s", path)), nil
	}
}

package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbListViewsSchema returns the JSON Schema for the db_list_views tool.
func DbListViewsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"schema": map[string]any{
				"type":        "string",
				"description": "Schema name (postgres only, defaults to 'public')",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// DbListViews returns a tool handler that lists views in a database.
func DbListViews(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		schema := helpers.GetStringOr(req.Arguments, "schema", "")

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		views, err := provider.ListViews(ctx, schema)
		if err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("Listing views is not supported by the %s provider.", provider.Kind())), nil
			}
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(views) == 0 {
			return helpers.TextResult("No views found."), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Views (%d)\n", len(views))
		for _, v := range views {
			fmt.Fprintf(&b, "\n### %s\n", v.Name)
			fmt.Fprintf(&b, "```sql\n%s\n```\n", v.Definition)
		}

		return helpers.TextResult(b.String()), nil
	}
}

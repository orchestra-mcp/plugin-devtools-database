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

// PgListExtensionsSchema returns the JSON Schema for the pg_list_extensions tool.
func PgListExtensionsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"available": map[string]any{
				"type":        "boolean",
				"description": "If true, show all available extensions; otherwise show only installed",
				"default":     false,
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgListExtensions returns a tool handler that lists PostgreSQL extensions.
func PgListExtensions(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		available := helpers.GetBool(req.Arguments, "available")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		exts, err := pg.ListExtensions(ctx, available)
		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(exts) == 0 {
			return helpers.TextResult("No extensions found."), nil
		}

		var b strings.Builder
		if available {
			fmt.Fprintf(&b, "## Available Extensions\n\n")
			fmt.Fprintf(&b, "| Name | Version | Schema | Installed | Comment |\n")
			fmt.Fprintf(&b, "|------|---------|--------|-----------|--------|\n")
			for _, e := range exts {
				installed := "No"
				if e.Installed {
					installed = "Yes"
				}
				fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n",
					e.Name, e.Version, e.Schema, installed, e.Comment)
			}
		} else {
			fmt.Fprintf(&b, "## Installed Extensions\n\n")
			fmt.Fprintf(&b, "| Name | Version | Schema |\n")
			fmt.Fprintf(&b, "|------|---------|--------|\n")
			for _, e := range exts {
				fmt.Fprintf(&b, "| %s | %s | %s |\n", e.Name, e.Version, e.Schema)
			}
		}

		return helpers.TextResult(b.String()), nil
	}
}

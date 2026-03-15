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

// DbListConstraintsSchema returns the JSON Schema for the db_list_constraints tool.
func DbListConstraintsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to list constraints for",
			},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

// DbListConstraints returns a tool handler that lists constraints on a table.
func DbListConstraints(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		constraints, err := provider.ListConstraints(ctx, table)
		if err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("Listing constraints is not supported by the %s provider.", provider.Kind())), nil
			}
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(constraints) == 0 {
			return helpers.TextResult(fmt.Sprintf("No constraints found on table %q.", table)), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Constraints on: %s\n\n", table)
		fmt.Fprintf(&b, "| Name | Type | Columns | References | Definition |\n")
		fmt.Fprintf(&b, "|------|------|---------|------------|------------|\n")
		for _, c := range constraints {
			refs := ""
			if c.RefTable != "" {
				refs = fmt.Sprintf("%s(%s)", c.RefTable, strings.Join(c.RefColumns, ", "))
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n",
				c.Name, c.Type, strings.Join(c.Columns, ", "), refs, c.Definition)
		}

		return helpers.TextResult(b.String()), nil
	}
}

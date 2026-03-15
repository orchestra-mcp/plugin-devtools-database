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

// PgListPublicationsSchema returns the JSON Schema for the pg_list_publications tool.
func PgListPublicationsSchema() *structpb.Struct {
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

// PgListPublications returns a tool handler that lists PostgreSQL logical replication publications.
func PgListPublications(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		pubs, err := pg.ListPublications(ctx)
		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(pubs) == 0 {
			return helpers.TextResult("No publications found."), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Publications\n\n")
		fmt.Fprintf(&b, "| Name | Owner | All Tables | Insert | Update | Delete | Tables |\n")
		fmt.Fprintf(&b, "|------|-------|------------|--------|--------|--------|--------|\n")
		for _, p := range pubs {
			tables := "ALL"
			if !p.AllTables {
				tables = strings.Join(p.Tables, ", ")
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n",
				p.Name, p.Owner,
				boolYesNo(p.AllTables), boolYesNo(p.Insert),
				boolYesNo(p.Update), boolYesNo(p.Delete),
				tables)
		}

		return helpers.TextResult(b.String()), nil
	}
}

func boolYesNo(v bool) string {
	if v {
		return "Yes"
	}
	return "No"
}

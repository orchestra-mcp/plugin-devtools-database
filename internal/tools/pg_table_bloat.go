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

// PgTableBloatSchema returns the JSON Schema for the pg_table_bloat tool.
func PgTableBloatSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name (if empty, shows all tables)",
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgTableBloat returns a tool handler that shows table bloat statistics.
func PgTableBloat(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		bloat, err := pg.TableBloat(ctx, table)
		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(bloat) == 0 {
			return helpers.TextResult("No bloat data available."), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Table Bloat\n\n")
		fmt.Fprintf(&b, "| Table | Size | Live Tuples | Dead Tuples | Bloat %% | Last Vacuum | Last Auto Vacuum |\n")
		fmt.Fprintf(&b, "|-------|------|-------------|-------------|---------|-------------|------------------|\n")
		for _, bl := range bloat {
			fmt.Fprintf(&b, "| %s.%s | %s | %d | %d | %.2f%% | %s | %s |\n",
				bl.SchemaName, bl.Name,
				formatBytes(bl.TableSize),
				bl.LiveTuples, bl.DeadTuples,
				bl.BloatRatio,
				bl.LastVacuum, bl.LastAutoVac)
		}

		return helpers.TextResult(b.String()), nil
	}
}

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

// PgIndexBloatSchema returns the JSON Schema for the pg_index_bloat tool.
func PgIndexBloatSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"min_bloat_pct": map[string]any{
				"type":        "number",
				"description": "Minimum bloat percentage to show",
				"default":     30,
			},
		},
		"required": []any{"connection_id"},
	})
	return s
}

// PgIndexBloat returns a tool handler that shows index bloat statistics.
func PgIndexBloat(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		minBloatPct := float64(30)
		if req.Arguments != nil {
			if f, ok := req.Arguments.Fields["min_bloat_pct"]; ok {
				minBloatPct = f.GetNumberValue()
			}
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		items, err := pg.IndexBloat(ctx, minBloatPct)
		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		if len(items) == 0 {
			return helpers.TextResult("No index bloat detected above threshold."), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Index Bloat (>= %.0f%%)\n\n", minBloatPct)
		fmt.Fprintf(&b, "| Index | Table | Index Size | Table Size | Bloat %% |\n")
		fmt.Fprintf(&b, "|-------|-------|------------|------------|--------|\n")
		for _, ib := range items {
			fmt.Fprintf(&b, "| %s | %s.%s | %s | %s | %.2f%% |\n",
				ib.IndexName,
				ib.SchemaName, ib.TableName,
				formatBytes(ib.IndexSize),
				formatBytes(ib.TableSize),
				ib.BloatPct)
		}

		return helpers.TextResult(b.String()), nil
	}
}

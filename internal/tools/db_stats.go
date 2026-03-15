package tools

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbStatsSchema returns the JSON Schema for the db_stats tool.
func DbStatsSchema() *structpb.Struct {
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

// DbStats returns a tool handler that shows database-level statistics.
func DbStats(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")

		provider, err := mgr.GetProvider(connID)
		if err != nil {
			return helpers.ErrorResult("connection_error", err.Error()), nil
		}

		stats, err := provider.DatabaseStats(ctx)
		if err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("Database statistics are not supported by the %s provider.", provider.Kind())), nil
			}
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Database Statistics\n\n")
		fmt.Fprintf(&b, "| Metric | Value |\n")
		fmt.Fprintf(&b, "|--------|-------|\n")
		fmt.Fprintf(&b, "| Provider | %s |\n", stats.Provider)
		if stats.Version != "" {
			fmt.Fprintf(&b, "| Version | %s |\n", stats.Version)
		}
		fmt.Fprintf(&b, "| Size | %s |\n", formatBytes(stats.SizeBytes))
		fmt.Fprintf(&b, "| Tables | %d |\n", stats.TableCount)
		fmt.Fprintf(&b, "| Indexes | %d |\n", stats.IndexCount)

		if len(stats.Extra) > 0 {
			// Sort keys for deterministic output.
			keys := make([]string, 0, len(stats.Extra))
			for k := range stats.Extra {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				fmt.Fprintf(&b, "| %s | %v |\n", k, stats.Extra[k])
			}
		}

		return helpers.TextResult(b.String()), nil
	}
}

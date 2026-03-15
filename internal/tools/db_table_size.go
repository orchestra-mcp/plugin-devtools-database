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

// DbTableSizeSchema returns the JSON Schema for the db_table_size tool.
func DbTableSizeSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to get size for",
			},
		},
		"required": []any{"connection_id", "table"},
	})
	return s
}

// DbTableSize returns a tool handler that shows size and row count for a table.
func DbTableSize(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
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

		stats, err := provider.TableSize(ctx, table)
		if err != nil {
			if errors.Is(err, db.ErrUnsupported) {
				return helpers.ErrorResult("unsupported", fmt.Sprintf("Table size is not supported by the %s provider.", provider.Kind())), nil
			}
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Table Size: %s\n\n", table)
		fmt.Fprintf(&b, "| Metric | Value |\n")
		fmt.Fprintf(&b, "|--------|-------|\n")
		fmt.Fprintf(&b, "| Rows | %s |\n", formatCount(stats.RowCount))
		fmt.Fprintf(&b, "| Data Size | %s |\n", formatBytes(stats.SizeBytes))
		fmt.Fprintf(&b, "| Index Size | %s |\n", formatBytes(stats.IndexSize))
		fmt.Fprintf(&b, "| Total Size | %s |\n", formatBytes(stats.TotalSize))

		return helpers.TextResult(b.String()), nil
	}
}

// formatBytes converts a byte count into a human-readable string (B, KB, MB, GB).
func formatBytes(b int64) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
	)

	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// formatCount formats an integer with comma separators.
func formatCount(n int64) string {
	if n < 0 {
		return "-" + formatCount(-n)
	}

	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var b strings.Builder
	remainder := len(s) % 3
	if remainder > 0 {
		b.WriteString(s[:remainder])
	}
	for i := remainder; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

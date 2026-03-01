package tools

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbExportSchema returns the JSON Schema for the db_export tool.
func DbExportSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Table name to export",
			},
			"format": map[string]any{
				"type":        "string",
				"description": "Export format (csv or json)",
				"enum":        []any{"csv", "json"},
			},
			"path": map[string]any{
				"type":        "string",
				"description": "Output file path (defaults to ./<table>.<format>)",
			},
		},
		"required": []any{"connection_id", "table", "format"},
	})
	return s
}

// DbExport returns a tool handler that exports a table as CSV or JSON.
func DbExport(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "format"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		format := helpers.GetString(req.Arguments, "format")

		if err := helpers.ValidateOneOf(format, "csv", "json"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		outPath := helpers.GetString(req.Arguments, "path")
		if outPath == "" {
			outPath = fmt.Sprintf("%s.%s", table, format)
		}

		// Query all rows from the table.
		rows, err := mgr.Query(connID, fmt.Sprintf("SELECT * FROM %s", table))
		if err != nil {
			return helpers.ErrorResult("query_error", err.Error()), nil
		}

		// Ensure parent directory exists.
		if dir := filepath.Dir(outPath); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return helpers.ErrorResult("io_error", fmt.Sprintf("create directory: %v", err)), nil
			}
		}

		switch format {
		case "csv":
			if err := exportCSV(outPath, rows); err != nil {
				return helpers.ErrorResult("export_error", err.Error()), nil
			}
		case "json":
			if err := exportJSON(outPath, rows); err != nil {
				return helpers.ErrorResult("export_error", err.Error()), nil
			}
		}

		return helpers.TextResult(fmt.Sprintf("Exported %d rows from %q to %s (%s format).", len(rows), table, outPath, format)), nil
	}
}

// exportCSV writes rows as CSV to the given path.
func exportCSV(path string, rows []map[string]any) error {
	if len(rows) == 0 {
		// Write an empty file.
		return os.WriteFile(path, []byte(""), 0o644)
	}

	// Collect and sort column names for deterministic output.
	colSet := make(map[string]struct{})
	for _, row := range rows {
		for k := range row {
			colSet[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(colSet))
	for k := range colSet {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header row.
	if err := w.Write(cols); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Data rows.
	for _, row := range rows {
		record := make([]string, len(cols))
		for i, col := range cols {
			record[i] = fmt.Sprintf("%v", row[col])
		}
		if err := w.Write(record); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}

	return nil
}

// exportJSON writes rows as a JSON array to the given path.
func exportJSON(path string, rows []map[string]any) error {
	data, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}

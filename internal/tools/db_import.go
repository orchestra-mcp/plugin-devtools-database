package tools

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbImportSchema returns the JSON Schema for the db_import tool.
func DbImportSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"table": map[string]any{
				"type":        "string",
				"description": "Target table name",
			},
			"path": map[string]any{
				"type":        "string",
				"description": "Path to the CSV or JSON file to import",
			},
			"format": map[string]any{
				"type":        "string",
				"description": "File format (csv or json). Auto-detected from extension if omitted.",
				"enum":        []any{"csv", "json"},
			},
		},
		"required": []any{"connection_id", "table", "path"},
	})
	return s
}

// DbImport returns a tool handler that imports data from a CSV or JSON file.
func DbImport(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table", "path"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		path := helpers.GetString(req.Arguments, "path")

		format := helpers.GetString(req.Arguments, "format")
		if format == "" {
			// Auto-detect from file extension.
			if strings.HasSuffix(path, ".csv") {
				format = "csv"
			} else if strings.HasSuffix(path, ".json") {
				format = "json"
			} else {
				return helpers.ErrorResult("validation_error", "cannot detect format from file extension; specify format explicitly"), nil
			}
		}

		if err := helpers.ValidateOneOf(format, "csv", "json"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		var rows []map[string]any
		var err error

		switch format {
		case "csv":
			rows, err = readCSV(path)
		case "json":
			rows, err = readJSON(path)
		}
		if err != nil {
			return helpers.ErrorResult("import_error", err.Error()), nil
		}

		if len(rows) == 0 {
			return helpers.TextResult("File contains no data rows."), nil
		}

		// Insert rows one by one using parameterized queries.
		inserted := 0
		for _, row := range rows {
			cols := make([]string, 0, len(row))
			vals := make([]any, 0, len(row))
			placeholders := make([]string, 0, len(row))

			conn, err := mgr.Get(connID)
			if err != nil {
				return helpers.ErrorResult("connection_error", err.Error()), nil
			}

			i := 1
			for k, v := range row {
				cols = append(cols, k)
				vals = append(vals, v)
				switch conn.Driver {
				case "postgres":
					placeholders = append(placeholders, fmt.Sprintf("$%d", i))
				default:
					placeholders = append(placeholders, "?")
				}
				i++
			}

			query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				table,
				strings.Join(cols, ", "),
				strings.Join(placeholders, ", "))

			if _, err := mgr.Exec(connID, query, vals...); err != nil {
				return helpers.ErrorResult("import_error",
					fmt.Sprintf("row %d: %v", inserted+1, err)), nil
			}
			inserted++
		}

		return helpers.TextResult(fmt.Sprintf("Imported %d rows into %q from %s.", inserted, table, path)), nil
	}
}

// readCSV reads a CSV file and returns rows as maps (header row = keys).
func readCSV(path string) ([]map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)

	// First row is the header.
	header, err := r.Read()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("read header: %w", err)
	}

	var rows []map[string]any
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read row: %w", err)
		}
		row := make(map[string]any, len(header))
		for i, col := range header {
			if i < len(record) {
				row[col] = record[i]
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// readJSON reads a JSON file containing an array of objects.
func readJSON(path string) ([]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	var rows []map[string]any
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("parse json: %w", err)
	}
	return rows, nil
}

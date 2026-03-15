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

func PgVectorSearchSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id":  map[string]any{"type": "string", "description": "Connection ID"},
			"table":          map[string]any{"type": "string", "description": "Table name"},
			"column":         map[string]any{"type": "string", "description": "Vector column name (default: embedding)"},
			"query_vector":   map[string]any{"type": "array", "items": map[string]any{"type": "number"}, "description": "Query vector as array of floats"},
			"distance":       map[string]any{"type": "string", "description": "Distance metric: cosine (default), l2, or ip"},
			"limit":          map[string]any{"type": "integer", "description": "Max results (default 10)"},
			"select_columns": map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Columns to return (default: all)"},
			"where":          map[string]any{"type": "string", "description": "Optional WHERE filter (raw SQL)"},
		},
		"required": []any{"connection_id", "table", "query_vector"},
	})
	return s
}

func PgVectorSearch(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "table"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}
		connID := helpers.GetString(req.Arguments, "connection_id")
		table := helpers.GetString(req.Arguments, "table")
		column := helpers.GetString(req.Arguments, "column")
		distance := helpers.GetString(req.Arguments, "distance")
		limit := helpers.GetInt(req.Arguments, "limit")
		where := helpers.GetString(req.Arguments, "where")

		// Parse query_vector from structpb list.
		queryVector, err := getFloatArray(req.Arguments, "query_vector")
		if err != nil || len(queryVector) == 0 {
			return helpers.ErrorResult("validation_error", "query_vector must be a non-empty array of numbers"), nil
		}

		// Parse select_columns.
		var selectColumns []string
		if v, ok := req.Arguments.Fields["select_columns"]; ok && v.GetListValue() != nil {
			for _, item := range v.GetListValue().Values {
				if s := item.GetStringValue(); s != "" {
					selectColumns = append(selectColumns, s)
				}
			}
		}

		pg, errResp := getPostgresProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		rows, err := pg.VectorSearch(ctx, table, column, queryVector, distance, limit, selectColumns, where)
		if err != nil {
			return helpers.ErrorResult("vector_search_error", err.Error()), nil
		}
		if len(rows) == 0 {
			return helpers.TextResult("No results found."), nil
		}
		return helpers.TextResult(formatQueryResults(rows)), nil
	}
}

// getFloatArray extracts a []float64 from a structpb field.
func getFloatArray(args *structpb.Struct, key string) ([]float64, error) {
	v, ok := args.Fields[key]
	if !ok || v.GetListValue() == nil {
		return nil, fmt.Errorf("missing %s", key)
	}
	list := v.GetListValue().Values
	result := make([]float64, len(list))
	for i, item := range list {
		result[i] = item.GetNumberValue()
	}
	return result, nil
}

// formatQueryResults formats rows as a simple markdown table.
func formatQueryResults(rows []map[string]any) string {
	if len(rows) == 0 {
		return "No results."
	}

	// Collect column names from first row.
	var cols []string
	for k := range rows[0] {
		cols = append(cols, k)
	}

	var b strings.Builder
	// Header.
	b.WriteString("| ")
	for _, c := range cols {
		b.WriteString(c)
		b.WriteString(" | ")
	}
	b.WriteString("\n|")
	for range cols {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	// Rows.
	for _, row := range rows {
		b.WriteString("| ")
		for _, c := range cols {
			fmt.Fprintf(&b, "%v | ", row[c])
		}
		b.WriteString("\n")
	}
	return b.String()
}

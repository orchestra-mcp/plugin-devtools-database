package tools

import (
	"context"
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbConnectSchema returns the JSON Schema for the db_connect tool.
func DbConnectSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"driver": map[string]any{
				"type":        "string",
				"description": "Database driver (postgres, sqlite3, mysql)",
				"enum":        []any{"postgres", "sqlite3", "mysql"},
			},
			"dsn": map[string]any{
				"type":        "string",
				"description": "Data source name / connection string",
			},
		},
		"required": []any{"driver", "dsn"},
	})
	return s
}

// DbConnect returns a tool handler that connects to a database.
func DbConnect(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "driver", "dsn"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		driver := helpers.GetString(req.Arguments, "driver")
		dsn := helpers.GetString(req.Arguments, "dsn")

		id, err := mgr.Connect(driver, dsn)
		if err != nil {
			return helpers.ErrorResult("connect_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf("Connected to %s database. Connection ID: %s", driver, id)), nil
	}
}

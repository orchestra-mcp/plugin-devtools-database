package tools

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// DbCreateDatabaseSchema returns the JSON Schema for the db_create_database tool.
func DbCreateDatabaseSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{
				"type":        "string",
				"description": "Absolute path for the new SQLite database file",
			},
			"name": map[string]any{
				"type":        "string",
				"description": "Human-readable name for the database (used in confirmation messages)",
			},
		},
		"required": []any{"path", "name"},
	})
	return s
}

// DbCreateDatabase returns a tool handler that creates a new SQLite database
// file and auto-connects to it.
func DbCreateDatabase(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "path", "name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		dbPath := helpers.GetString(req.Arguments, "path")
		dbName := helpers.GetString(req.Arguments, "name")

		// Check if file already exists.
		if _, err := os.Stat(dbPath); err == nil {
			return helpers.ErrorResult("exists_error",
				fmt.Sprintf("Database file already exists at %s. Use db_connect to connect to it.", dbPath)), nil
		}

		// Ensure parent directory exists.
		dir := filepath.Dir(dbPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return helpers.ErrorResult("mkdir_error",
				fmt.Sprintf("Failed to create directory %s: %v", dir, err)), nil
		}

		// Connect creates the SQLite file if it doesn't exist.
		id, err := mgr.Connect("sqlite", dbPath)
		if err != nil {
			return helpers.ErrorResult("create_error", err.Error()), nil
		}

		return helpers.TextResult(fmt.Sprintf(
			"Created database %q at %s.\nConnection ID: %s\n\nThe database is empty and ready for use. Run SQL CREATE TABLE statements via db_query to set up your schema.",
			dbName, dbPath, id)), nil
	}
}

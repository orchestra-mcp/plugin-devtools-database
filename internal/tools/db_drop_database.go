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

// DbDropDatabaseSchema returns the JSON Schema for the db_drop_database tool.
func DbDropDatabaseSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{
				"type":        "string",
				"description": "Absolute path to the SQLite database file to delete",
			},
			"confirm_name": map[string]any{
				"type":        "string",
				"description": "Type the database file name (without path) to confirm deletion. This MUST match the actual file name.",
			},
		},
		"required": []any{"path", "confirm_name"},
	})
	return s
}

// DbDropDatabase returns a tool handler that deletes a SQLite database file.
// Requires the user to confirm by providing the exact file name.
// IMPORTANT: The calling agent MUST use AskUserQuestion to get user confirmation
// before calling this tool.
func DbDropDatabase(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "path", "confirm_name"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		dbPath := helpers.GetString(req.Arguments, "path")
		confirmName := helpers.GetString(req.Arguments, "confirm_name")

		// Verify the file exists.
		info, err := os.Stat(dbPath)
		if os.IsNotExist(err) {
			return helpers.ErrorResult("not_found", fmt.Sprintf("Database file not found: %s", dbPath)), nil
		}
		if err != nil {
			return helpers.ErrorResult("stat_error", err.Error()), nil
		}
		if info.IsDir() {
			return helpers.ErrorResult("invalid_path", fmt.Sprintf("%s is a directory, not a database file", dbPath)), nil
		}

		// Verify confirmation name matches the actual file name.
		actualName := filepath.Base(dbPath)
		if confirmName != actualName {
			return helpers.ErrorResult("confirm_mismatch",
				fmt.Sprintf("Confirmation failed: you typed %q but the file name is %q. Ask the user to confirm the correct name.", confirmName, actualName)), nil
		}

		// Disconnect any active connections to this database.
		for _, conn := range mgr.List() {
			if conn.DSN == dbPath {
				mgr.Disconnect(conn.ID)
			}
		}

		// Delete the database file.
		if err := os.Remove(dbPath); err != nil {
			return helpers.ErrorResult("delete_error", err.Error()), nil
		}

		// Also remove WAL and SHM files if they exist (SQLite journal files).
		os.Remove(dbPath + "-wal")
		os.Remove(dbPath + "-shm")

		return helpers.TextResult(fmt.Sprintf(
			"Dropped database %q.\nDeleted: %s\nAny active connections to this database have been closed.",
			actualName, dbPath)), nil
	}
}

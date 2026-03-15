package tools

import (
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db/providers"
	"github.com/orchestra-mcp/sdk-go/helpers"
)

// getPostgresProvider extracts a *PostgresProvider from the connection,
// returning a user-friendly error response if the connection is not PostgreSQL.
func getPostgresProvider(mgr *db.Manager, connID string) (*providers.PostgresProvider, *pluginv1.ToolResponse) {
	provider, err := mgr.GetProvider(connID)
	if err != nil {
		return nil, helpers.ErrorResult("connection_error", err.Error())
	}
	pg, ok := provider.(*providers.PostgresProvider)
	if !ok {
		return nil, helpers.ErrorResult("postgres_only",
			fmt.Sprintf("This tool requires a PostgreSQL connection. Current provider: %s", provider.Kind()))
	}
	return pg, nil
}

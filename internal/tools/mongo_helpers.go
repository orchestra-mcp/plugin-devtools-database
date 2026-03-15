package tools

import (
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db/providers"
	"github.com/orchestra-mcp/sdk-go/helpers"
)

// getMongoDBProvider extracts a *MongoDBProvider from the connection,
// returning a user-friendly error response if the connection is not MongoDB.
func getMongoDBProvider(mgr *db.Manager, connID string) (*providers.MongoDBProvider, *pluginv1.ToolResponse) {
	provider, err := mgr.GetProvider(connID)
	if err != nil {
		return nil, helpers.ErrorResult("connection_error", err.Error())
	}
	mp, ok := provider.(*providers.MongoDBProvider)
	if !ok {
		return nil, helpers.ErrorResult("mongodb_only",
			fmt.Sprintf("This tool requires a MongoDB connection. Current provider: %s", provider.Kind()))
	}
	return mp, nil
}

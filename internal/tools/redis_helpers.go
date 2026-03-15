package tools

import (
	"fmt"

	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db/providers"
	"github.com/orchestra-mcp/sdk-go/helpers"
)

// getRedisProvider extracts a *RedisProvider from the connection,
// returning a user-friendly error response if the connection is not Redis.
func getRedisProvider(mgr *db.Manager, connID string) (*providers.RedisProvider, *pluginv1.ToolResponse) {
	provider, err := mgr.GetProvider(connID)
	if err != nil {
		return nil, helpers.ErrorResult("connection_error", err.Error())
	}
	rp, ok := provider.(*providers.RedisProvider)
	if !ok {
		return nil, helpers.ErrorResult("redis_only",
			fmt.Sprintf("This tool requires a Redis connection. Current provider: %s", provider.Kind()))
	}
	return rp, nil
}

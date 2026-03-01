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

// DbListConnectionsSchema returns the JSON Schema for the db_list_connections tool.
func DbListConnectionsSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	})
	return s
}

// DbListConnections returns a tool handler that lists all active database connections.
func DbListConnections(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		conns := mgr.List()

		if len(conns) == 0 {
			return helpers.TextResult("No active connections."), nil
		}

		var b strings.Builder
		fmt.Fprintf(&b, "## Active Connections (%d)\n\n", len(conns))
		fmt.Fprintf(&b, "| ID | Driver | DSN |\n")
		fmt.Fprintf(&b, "|----|--------|-----|\n")
		for _, c := range conns {
			fmt.Fprintf(&b, "| %s | %s | %s |\n", c.ID, c.Driver, c.DSN)
		}

		return helpers.TextResult(b.String()), nil
	}
}

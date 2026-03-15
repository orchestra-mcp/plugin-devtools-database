package devtoolsdatabase

import (
	"github.com/orchestra-mcp/plugin-devtools-database/internal"
	"github.com/orchestra-mcp/sdk-go/plugin"

	// Register database drivers so they're available when the plugin
	// is loaded in-process.
	_ "github.com/go-sql-driver/mysql" // registers as "mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // registers as "pgx" (aliased to "postgres" in manager.go)
	_ "modernc.org/sqlite"             // registers as "sqlite"
)

// Register adds all database devtools to the builder.
func Register(builder *plugin.PluginBuilder) {
	tp := &internal.ToolsPlugin{}
	tp.RegisterTools(builder)
}

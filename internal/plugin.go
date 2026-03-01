package internal

import (
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/tools"
	"github.com/orchestra-mcp/sdk-go/plugin"
)

// ToolsPlugin registers all database devtools.
type ToolsPlugin struct{}

// RegisterTools registers all 8 database tools with the plugin builder.
func (tp *ToolsPlugin) RegisterTools(builder *plugin.PluginBuilder) {
	mgr := db.NewManager()

	builder.RegisterTool("db_connect",
		"Connect to a database (postgres, sqlite3, mysql)",
		tools.DbConnectSchema(), tools.DbConnect(mgr))

	builder.RegisterTool("db_disconnect",
		"Disconnect from a database",
		tools.DbDisconnectSchema(), tools.DbDisconnect(mgr))

	builder.RegisterTool("db_query",
		"Execute a SELECT query and return results as JSON",
		tools.DbQuerySchema(), tools.DbQuery(mgr))

	builder.RegisterTool("db_list_tables",
		"List tables in a database",
		tools.DbListTablesSchema(), tools.DbListTables(mgr))

	builder.RegisterTool("db_describe_table",
		"Describe table columns and types",
		tools.DbDescribeTableSchema(), tools.DbDescribeTable(mgr))

	builder.RegisterTool("db_list_connections",
		"List all active database connections",
		tools.DbListConnectionsSchema(), tools.DbListConnections(mgr))

	builder.RegisterTool("db_export",
		"Export a table as CSV or JSON",
		tools.DbExportSchema(), tools.DbExport(mgr))

	builder.RegisterTool("db_import",
		"Import data from a CSV or JSON file into a table",
		tools.DbImportSchema(), tools.DbImport(mgr))
}

package devtoolsdatabase

import (
	"github.com/orchestra-mcp/plugin-devtools-database/internal"
	"github.com/orchestra-mcp/sdk-go/plugin"
)

// Register adds all database devtools to the builder.
func Register(builder *plugin.PluginBuilder) {
	tp := &internal.ToolsPlugin{}
	tp.RegisterTools(builder)
}

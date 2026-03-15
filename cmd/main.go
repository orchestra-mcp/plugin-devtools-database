package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/orchestra-mcp/plugin-devtools-database/internal"
	"github.com/orchestra-mcp/sdk-go/plugin"

	// Register database drivers.
	_ "github.com/go-sql-driver/mysql" // mysql driver (driver name: "mysql")
	_ "modernc.org/sqlite"             // sqlite driver (driver name: "sqlite")
)

func main() {
	builder := plugin.New("devtools.database").
		Version("0.1.0").
		Description("Database devtools — connect, query, inspect, import/export").
		Author("Orchestra").
		Binary("devtools-database")

	tp := &internal.ToolsPlugin{}
	tp.RegisterTools(builder)

	p := builder.BuildWithTools()
	p.ParseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	if err := p.Run(ctx); err != nil {
		log.Fatalf("devtools.database: %v", err)
	}
}

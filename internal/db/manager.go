package db

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
)

// ProviderFactory creates a Provider from a *sql.DB connection.
type ProviderFactory func(sqlDB *sql.DB) Provider

// providerFactories maps driver names to their factory functions.
var providerFactories = map[string]ProviderFactory{}

// RegisterProviderFactory registers a factory for the given driver name.
// This is called from provider init() functions to avoid import cycles.
func RegisterProviderFactory(driver string, factory ProviderFactory) {
	providerFactories[driver] = factory
}

// NonSQLProviderFactory creates a Provider directly from a DSN string.
// Used for non-SQL databases (e.g. Redis) that don't use database/sql.
type NonSQLProviderFactory func(dsn string) (Provider, error)

// nonSQLFactories maps driver names to their non-SQL factory functions.
var nonSQLFactories = map[string]NonSQLProviderFactory{}

// RegisterNonSQLProviderFactory registers a factory for a non-SQL driver.
func RegisterNonSQLProviderFactory(driver string, factory NonSQLProviderFactory) {
	nonSQLFactories[driver] = factory
}

// Connection represents an active database connection.
type Connection struct {
	ID       string
	Driver   string
	DSN      string
	DB       *sql.DB
	Provider Provider
}

// ConnectionInfo is a read-only summary of a connection (no *sql.DB exposed).
type ConnectionInfo struct {
	ID     string `json:"id"`
	Driver string `json:"driver"`
	DSN    string `json:"dsn"`
}

// Manager manages multiple database connections.
type Manager struct {
	connections map[string]*Connection
	mu          sync.RWMutex
}

// NewManager creates a new Manager with an empty connection map.
func NewManager() *Manager {
	return &Manager{
		connections: make(map[string]*Connection),
	}
}

// Connect opens a new database connection with the given driver and DSN.
// Supported drivers: "postgres", "sqlite3", "sqlite", "mysql", "redis".
// Returns the connection ID on success.
func (m *Manager) Connect(driver, dsn string) (string, error) {
	// Check non-SQL factories first (e.g. Redis).
	if factory, ok := nonSQLFactories[driver]; ok {
		provider, err := factory(dsn)
		if err != nil {
			return "", fmt.Errorf("connect %s: %w", driver, err)
		}

		id := generateID()
		m.mu.Lock()
		m.connections[id] = &Connection{
			ID:       id,
			Driver:   driver,
			DSN:      dsn,
			DB:       nil,
			Provider: provider,
		}
		m.mu.Unlock()
		return id, nil
	}

	// SQL path.
	switch driver {
	case "postgres", "sqlite3", "sqlite", "mysql":
	default:
		return "", fmt.Errorf("unsupported driver %q (supported: postgres, sqlite, mysql, redis, mongodb)", driver)
	}

	// Normalize driver name for sql.Open():
	// - modernc.org/sqlite registers as "sqlite"
	// - jackc/pgx registers as "pgx"
	openDriver := driver
	if driver == "sqlite3" {
		openDriver = "sqlite"
	} else if driver == "postgres" {
		openDriver = "pgx"
	}

	sqlDB, err := sql.Open(openDriver, dsn)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", driver, err)
	}

	if err := sqlDB.Ping(); err != nil {
		sqlDB.Close()
		return "", fmt.Errorf("ping %s: %w", driver, err)
	}

	// Resolve factory key: sqlite3 and sqlite both use "sqlite".
	factoryKey := driver
	if factoryKey == "sqlite3" {
		factoryKey = "sqlite"
	}

	factory, ok := providerFactories[factoryKey]
	if !ok {
		sqlDB.Close()
		return "", fmt.Errorf("no provider factory registered for driver %q", driver)
	}

	provider := factory(sqlDB)
	id := generateID()

	m.mu.Lock()
	m.connections[id] = &Connection{
		ID:       id,
		Driver:   driver,
		DSN:      dsn,
		DB:       sqlDB,
		Provider: provider,
	}
	m.mu.Unlock()

	return id, nil
}

// Disconnect closes and removes the connection with the given ID.
func (m *Manager) Disconnect(id string) error {
	m.mu.Lock()
	conn, ok := m.connections[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("connection %q not found", id)
	}
	delete(m.connections, id)
	m.mu.Unlock()

	return conn.Provider.Close()
}

// Get returns the connection with the given ID or an error if not found.
func (m *Manager) Get(id string) (*Connection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conn, ok := m.connections[id]
	if !ok {
		return nil, fmt.Errorf("connection %q not found", id)
	}
	return conn, nil
}

// GetProvider returns the Provider for the connection with the given ID.
func (m *Manager) GetProvider(id string) (Provider, error) {
	conn, err := m.Get(id)
	if err != nil {
		return nil, err
	}
	return conn.Provider, nil
}

// List returns info about all active connections.
func (m *Manager) List() []ConnectionInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	infos := make([]ConnectionInfo, 0, len(m.connections))
	for _, conn := range m.connections {
		infos = append(infos, ConnectionInfo{
			ID:     conn.ID,
			Driver: conn.Driver,
			DSN:    conn.DSN,
		})
	}
	return infos
}

// Query executes a SELECT query on the given connection and returns rows as
// a slice of maps. Delegates to the connection's Provider.
func (m *Manager) Query(id, query string, args ...any) ([]map[string]any, error) {
	conn, err := m.Get(id)
	if err != nil {
		return nil, err
	}

	return conn.Provider.Query(context.Background(), query, args...)
}

// Exec executes a non-SELECT statement (INSERT, UPDATE, DELETE, etc.) on the
// given connection and returns the number of rows affected. Delegates to the
// connection's Provider.
func (m *Manager) Exec(id, query string, args ...any) (int64, error) {
	conn, err := m.Get(id)
	if err != nil {
		return 0, err
	}

	return conn.Provider.Exec(context.Background(), query, args...)
}

// generateID returns a connection ID in the form "db-" + 6 random hex chars.
func generateID() string {
	b := make([]byte, 3)
	rand.Read(b)
	return "db-" + hex.EncodeToString(b)
}

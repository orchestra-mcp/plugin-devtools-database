package db

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
)

// Connection represents an active database connection.
type Connection struct {
	ID     string
	Driver string
	DSN    string
	DB     *sql.DB
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
// Supported drivers: "postgres", "sqlite3", "mysql".
// Returns the connection ID on success.
func (m *Manager) Connect(driver, dsn string) (string, error) {
	switch driver {
	case "postgres", "sqlite3", "sqlite", "mysql":
	default:
		return "", fmt.Errorf("unsupported driver %q (supported: postgres, sqlite3, sqlite, mysql)", driver)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", driver, err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return "", fmt.Errorf("ping %s: %w", driver, err)
	}

	id := generateID()

	m.mu.Lock()
	m.connections[id] = &Connection{
		ID:     id,
		Driver: driver,
		DSN:    dsn,
		DB:     db,
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

	return conn.DB.Close()
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
// a slice of maps. Each map key is a column name, each value is the column
// value (as returned by sql.Rows.Scan into *any).
func (m *Manager) Query(id, query string, args ...any) ([]map[string]any, error) {
	conn, err := m.Get(id)
	if err != nil {
		return nil, err
	}

	rows, err := conn.DB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		pointers := make([]any, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			val := values[i]
			// Convert []byte to string for JSON serialization.
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			row[col] = val
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return results, nil
}

// Exec executes a non-SELECT statement (INSERT, UPDATE, DELETE, etc.) on the
// given connection and returns the number of rows affected.
func (m *Manager) Exec(id, query string, args ...any) (int64, error) {
	conn, err := m.Get(id)
	if err != nil {
		return 0, err
	}

	result, err := conn.DB.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("exec: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return affected, nil
}

// generateID returns a connection ID in the form "db-" + 6 random hex chars.
func generateID() string {
	b := make([]byte, 3)
	rand.Read(b)
	return "db-" + hex.EncodeToString(b)
}

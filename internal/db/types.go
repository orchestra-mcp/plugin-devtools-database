package db

// MapCanonicalType converts a canonical column type to the native type for the given provider.
func MapCanonicalType(canonical string, provider ProviderKind) string {
	// Map based on provider
	m, ok := typeMap[provider]
	if !ok {
		return canonical
	}
	native, ok := m[canonical]
	if !ok {
		return canonical // passthrough unknown types as-is
	}
	return native
}

// ValidCanonicalTypes lists all accepted canonical types.
var ValidCanonicalTypes = []string{
	"string", "text", "integer", "bigint", "float", "decimal",
	"boolean", "timestamp", "date", "json", "blob", "uuid", "serial",
	"vector", "tsvector",
}

// IsValidCanonicalType checks if a type string is a known canonical type.
func IsValidCanonicalType(t string) bool {
	for _, v := range ValidCanonicalTypes {
		if v == t {
			return true
		}
	}
	return false
}

var typeMap = map[ProviderKind]map[string]string{
	ProviderSQLite: {
		"string":    "TEXT",
		"text":      "TEXT",
		"integer":   "INTEGER",
		"bigint":    "INTEGER",
		"float":     "REAL",
		"decimal":   "REAL",
		"boolean":   "INTEGER",
		"timestamp": "TEXT",
		"date":      "TEXT",
		"json":      "TEXT",
		"blob":      "BLOB",
		"uuid":      "TEXT",
		"serial":    "INTEGER",
		"vector":    "BLOB",
		"tsvector":  "TEXT",
	},
	ProviderPostgres: {
		"string":    "VARCHAR(255)",
		"text":      "TEXT",
		"integer":   "INTEGER",
		"bigint":    "BIGINT",
		"float":     "DOUBLE PRECISION",
		"decimal":   "NUMERIC",
		"boolean":   "BOOLEAN",
		"timestamp": "TIMESTAMPTZ",
		"date":      "DATE",
		"json":      "JSONB",
		"blob":      "BYTEA",
		"uuid":      "UUID",
		"serial":    "SERIAL",
		"vector":    "vector",
		"tsvector":  "tsvector",
	},
	ProviderMySQL: {
		"string":    "VARCHAR(255)",
		"text":      "TEXT",
		"integer":   "INT",
		"bigint":    "BIGINT",
		"float":     "DOUBLE",
		"decimal":   "DECIMAL(10,2)",
		"boolean":   "TINYINT(1)",
		"timestamp": "DATETIME",
		"date":      "DATE",
		"json":      "JSON",
		"blob":      "LONGBLOB",
		"uuid":      "CHAR(36)",
		"serial":    "INT AUTO_INCREMENT",
		"vector":    "LONGBLOB",
		"tsvector":  "TEXT",
	},
}

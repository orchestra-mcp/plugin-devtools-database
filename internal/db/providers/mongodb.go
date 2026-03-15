package providers

import (
	"context"
	"fmt"
	"strings"

	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoDBProvider implements db.Provider for MongoDB.
type MongoDBProvider struct {
	client *mongo.Client
	dbName string
}

func init() {
	db.RegisterNonSQLProviderFactory("mongodb", func(dsn string) (db.Provider, error) {
		client, err := mongo.Connect(options.Client().ApplyURI(dsn))
		if err != nil {
			return nil, fmt.Errorf("connect mongodb: %w", err)
		}

		if err := client.Ping(context.Background(), nil); err != nil {
			client.Disconnect(context.Background())
			return nil, fmt.Errorf("ping mongodb: %w", err)
		}

		// Extract database name from DSN path.
		dbName := extractDBName(dsn)
		if dbName == "" {
			dbName = "test"
		}

		return &MongoDBProvider{client: client, dbName: dbName}, nil
	})
}

// extractDBName extracts the database name from a MongoDB URI.
// e.g. "mongodb://localhost:27017/mydb" → "mydb"
func extractDBName(dsn string) string {
	// Remove scheme.
	s := dsn
	if idx := strings.Index(s, "://"); idx >= 0 {
		s = s[idx+3:]
	}
	// Remove auth (user:pass@).
	if idx := strings.LastIndex(s, "@"); idx >= 0 {
		s = s[idx+1:]
	}
	// Remove query string.
	if idx := strings.Index(s, "?"); idx >= 0 {
		s = s[:idx]
	}
	// Find path after host:port.
	if idx := strings.Index(s, "/"); idx >= 0 {
		name := s[idx+1:]
		if name != "" {
			return name
		}
	}
	return ""
}

// Client returns the underlying mongo.Client.
func (m *MongoDBProvider) Client() *mongo.Client { return m.client }

// Database returns the mongo.Database for this connection.
func (m *MongoDBProvider) Database() *mongo.Database { return m.client.Database(m.dbName) }

// DBName returns the database name.
func (m *MongoDBProvider) DBName() string { return m.dbName }

func (m *MongoDBProvider) Kind() db.ProviderKind { return db.ProviderMongoDB }

func (m *MongoDBProvider) Close() error {
	return m.client.Disconnect(context.Background())
}

func (m *MongoDBProvider) Ping(ctx context.Context) error {
	return m.client.Ping(ctx, nil)
}

// Query interprets the query string as a collection name and uses args[0] as a JSON filter string.
// Returns matching documents as maps.
func (m *MongoDBProvider) Query(ctx context.Context, query string, args ...any) ([]map[string]any, error) {
	collection := strings.TrimSpace(query)
	if collection == "" {
		return nil, fmt.Errorf("query must be a collection name")
	}

	filter := bson.M{}
	if len(args) > 0 {
		if filterStr, ok := args[0].(string); ok && filterStr != "" {
			if err := bson.UnmarshalExtJSON([]byte(filterStr), true, &filter); err != nil {
				return nil, fmt.Errorf("parse filter: %w", err)
			}
		}
	}

	cursor, err := m.Database().Collection(collection).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	rows := make([]map[string]any, len(results))
	for i, doc := range results {
		rows[i] = bsonMToMap(doc)
	}
	return rows, nil
}

// Exec runs a database command (e.g. insert, update, delete) via RunCommand.
// query is the command JSON. Returns 1 on success.
func (m *MongoDBProvider) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	var cmd bson.D
	if err := bson.UnmarshalExtJSON([]byte(query), true, &cmd); err != nil {
		return 0, fmt.Errorf("parse command: %w", err)
	}

	var result bson.M
	if err := m.Database().RunCommand(ctx, cmd).Decode(&result); err != nil {
		return 0, err
	}
	return 1, nil
}

// ListTables returns collection names.
func (m *MongoDBProvider) ListTables(ctx context.Context, _ string) ([]db.TableInfo, error) {
	names, err := m.Database().ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	tables := make([]db.TableInfo, len(names))
	for i, name := range names {
		tables[i] = db.TableInfo{Name: name}
	}
	return tables, nil
}

// DescribeTable samples the first document and infers fields/types.
func (m *MongoDBProvider) DescribeTable(ctx context.Context, table string) ([]db.ColumnInfo, error) {
	var doc bson.M
	err := m.Database().Collection(table).FindOne(ctx, bson.M{}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return []db.ColumnInfo{}, nil
		}
		return nil, err
	}

	cols := make([]db.ColumnInfo, 0, len(doc))
	for k, v := range doc {
		cols = append(cols, db.ColumnInfo{
			Name:     k,
			Type:     fmt.Sprintf("%T", v),
			Nullable: "YES",
		})
	}
	return cols, nil
}

// ListIndexes returns indexes on a collection.
func (m *MongoDBProvider) ListIndexes(ctx context.Context, table string) ([]db.IndexInfo, error) {
	cursor, err := m.Database().Collection(table).Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var indexes []db.IndexInfo
	for cursor.Next(ctx) {
		var idx bson.M
		if err := cursor.Decode(&idx); err != nil {
			continue
		}

		name, _ := idx["name"].(string)
		unique, _ := idx["unique"].(bool)

		var cols []string
		if key, ok := idx["key"].(bson.M); ok {
			for k := range key {
				cols = append(cols, k)
			}
		}

		indexes = append(indexes, db.IndexInfo{
			Name:    name,
			Columns: cols,
			Unique:  unique,
		})
	}
	return indexes, nil
}

func (m *MongoDBProvider) ListConstraints(context.Context, string) ([]db.ConstraintInfo, error) {
	return nil, db.ErrUnsupported
}

// ListViews returns views (collections with type "view").
func (m *MongoDBProvider) ListViews(ctx context.Context, _ string) ([]db.ViewInfo, error) {
	cursor, err := m.Database().ListCollections(ctx, bson.M{"type": "view"})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var views []db.ViewInfo
	for cursor.Next(ctx) {
		var info bson.M
		if err := cursor.Decode(&info); err != nil {
			continue
		}
		name, _ := info["name"].(string)
		viewOn := ""
		if opts, ok := info["options"].(bson.M); ok {
			viewOn, _ = opts["viewOn"].(string)
		}
		views = append(views, db.ViewInfo{
			Name:       name,
			Definition: fmt.Sprintf("VIEW ON %s", viewOn),
		})
	}
	return views, nil
}

// TableSize returns collection stats (document count + storage size).
func (m *MongoDBProvider) TableSize(ctx context.Context, table string) (*db.TableStats, error) {
	var result bson.M
	err := m.Database().RunCommand(ctx, bson.D{{Key: "collStats", Value: table}}).Decode(&result)
	if err != nil {
		return nil, err
	}

	count := mongoToInt64(result["count"])
	size := mongoToInt64(result["size"])
	indexSize := mongoToInt64(result["totalIndexSize"])

	return &db.TableStats{
		RowCount:  count,
		SizeBytes: size,
		IndexSize: indexSize,
		TotalSize: size + indexSize,
	}, nil
}

// DatabaseStats returns database-level stats.
func (m *MongoDBProvider) DatabaseStats(ctx context.Context) (*db.DbStats, error) {
	var result bson.M
	err := m.Database().RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&result)
	if err != nil {
		return nil, err
	}

	var buildInfo bson.M
	m.Database().RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo)

	version, _ := buildInfo["version"].(string)
	dataSize := mongoToInt64(result["dataSize"])
	collections := mongoToInt64(result["collections"])
	indexes := mongoToInt64(result["indexes"])

	extra := map[string]any{
		"database":   m.dbName,
		"objects":    result["objects"],
		"avgObjSize": result["avgObjSize"],
		"indexes":    result["indexes"],
	}

	return &db.DbStats{
		SizeBytes:  dataSize,
		TableCount: int(collections),
		IndexCount: int(indexes),
		Provider:   "mongodb",
		Version:    version,
		Extra:      extra,
	}, nil
}

// CreateTable creates a new collection.
func (m *MongoDBProvider) CreateTable(ctx context.Context, name string, _ []db.ColumnDef, _ bool) error {
	return m.Database().CreateCollection(ctx, name)
}

func (m *MongoDBProvider) AlterTableAdd(context.Context, string, db.ColumnDef) error {
	return db.ErrUnsupported
}

func (m *MongoDBProvider) AlterTableDrop(context.Context, string, string) error {
	return db.ErrUnsupported
}

func (m *MongoDBProvider) AlterTableRename(context.Context, string, string, string) error {
	return db.ErrUnsupported
}

// DropTable drops a collection.
func (m *MongoDBProvider) DropTable(ctx context.Context, name string, _ bool) error {
	return m.Database().Collection(name).Drop(ctx)
}

// CreateIndex creates an index on a collection.
func (m *MongoDBProvider) CreateIndex(ctx context.Context, table string, index db.IndexDef) error {
	keys := bson.D{}
	for _, col := range index.Columns {
		keys = append(keys, bson.E{Key: col, Value: 1})
	}

	opts := options.Index().SetName(index.Name)
	if index.Unique {
		opts.SetUnique(true)
	}

	_, err := m.Database().Collection(table).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    keys,
		Options: opts,
	})
	return err
}

// DropIndex drops an index from a collection.
func (m *MongoDBProvider) DropIndex(ctx context.Context, table, indexName string) error {
	return m.Database().Collection(table).Indexes().DropOne(ctx, indexName)
}

// CreateView creates a MongoDB view.
func (m *MongoDBProvider) CreateView(ctx context.Context, view db.ViewDef) error {
	// Definition should be "viewOn:collectionName" for MongoDB views.
	// We'll use RunCommand to create the view with an empty pipeline.
	cmd := bson.D{
		{Key: "create", Value: view.Name},
		{Key: "viewOn", Value: view.Definition},
		{Key: "pipeline", Value: bson.A{}},
	}

	return m.Database().RunCommand(ctx, cmd).Err()
}

// DropView drops a view (same as dropping a collection in MongoDB).
func (m *MongoDBProvider) DropView(ctx context.Context, name string) error {
	return m.Database().Collection(name).Drop(ctx)
}

// --- Helpers ---

// bsonMToMap converts bson.M to map[string]any, stringifying ObjectID and other BSON types.
func bsonMToMap(doc bson.M) map[string]any {
	result := make(map[string]any, len(doc))
	for k, v := range doc {
		switch val := v.(type) {
		case bson.M:
			result[k] = bsonMToMap(val)
		case bson.A:
			arr := make([]any, len(val))
			for i, item := range val {
				if m, ok := item.(bson.M); ok {
					arr[i] = bsonMToMap(m)
				} else {
					arr[i] = fmt.Sprintf("%v", item)
				}
			}
			result[k] = arr
		default:
			result[k] = fmt.Sprintf("%v", val)
		}
	}
	return result
}

// mongoToInt64 converts a BSON numeric value to int64.
func mongoToInt64(v any) int64 {
	switch n := v.(type) {
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}

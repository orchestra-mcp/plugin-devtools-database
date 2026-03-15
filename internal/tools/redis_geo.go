package tools

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	pluginv1 "github.com/orchestra-mcp/gen-go/orchestra/plugin/v1"
	"github.com/orchestra-mcp/plugin-devtools-database/internal/db"
	"github.com/orchestra-mcp/sdk-go/helpers"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisGeoSchema returns the JSON Schema for the redis_geo tool.
func RedisGeoSchema() *structpb.Struct {
	s, _ := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connection_id": map[string]any{
				"type":        "string",
				"description": "Connection ID",
			},
			"action": map[string]any{
				"type":        "string",
				"description": "Geo command to execute",
				"enum":        []any{"geoadd", "geodist", "geopos", "geosearch"},
			},
			"key": map[string]any{
				"type":        "string",
				"description": "Geo key",
			},
			"longitude": map[string]any{
				"type":        "number",
				"description": "Longitude for GEOADD/GEOSEARCH",
			},
			"latitude": map[string]any{
				"type":        "number",
				"description": "Latitude for GEOADD/GEOSEARCH",
			},
			"member": map[string]any{
				"type":        "string",
				"description": "Member name for GEOADD/GEODIST/GEOPOS",
			},
			"member2": map[string]any{
				"type":        "string",
				"description": "Second member for GEODIST",
			},
			"members": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Members for GEOPOS",
			},
			"unit": map[string]any{
				"type":        "string",
				"description": "Unit for GEODIST/GEOSEARCH: m, km, mi, ft (default m)",
			},
			"radius": map[string]any{
				"type":        "number",
				"description": "Search radius for GEOSEARCH",
			},
			"count": map[string]any{
				"type":        "number",
				"description": "Max results for GEOSEARCH",
			},
		},
		"required": []any{"connection_id", "action"},
	})
	return s
}

// RedisGeo returns a tool handler for Redis geospatial commands.
func RedisGeo(mgr *db.Manager) func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
	return func(ctx context.Context, req *pluginv1.ToolRequest) (*pluginv1.ToolResponse, error) {
		if err := helpers.ValidateRequired(req.Arguments, "connection_id", "action"); err != nil {
			return helpers.ErrorResult("validation_error", err.Error()), nil
		}

		connID := helpers.GetString(req.Arguments, "connection_id")
		action := helpers.GetString(req.Arguments, "action")
		key := helpers.GetString(req.Arguments, "key")

		rp, errResp := getRedisProvider(mgr, connID)
		if errResp != nil {
			return errResp, nil
		}

		client := rp.Client()

		switch action {
		case "geoadd":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for GEOADD"), nil
			}
			member := helpers.GetString(req.Arguments, "member")
			if member == "" {
				return helpers.ErrorResult("validation_error", "member is required for GEOADD"), nil
			}
			longitude := helpers.GetFloat64(req.Arguments, "longitude")
			latitude := helpers.GetFloat64(req.Arguments, "latitude")
			added, err := client.GeoAdd(ctx, key, &redis.GeoLocation{
				Longitude: longitude,
				Latitude:  latitude,
				Name:      member,
			}).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("GEOADD: %d member(s) added to %q", added, key)), nil

		case "geodist":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for GEODIST"), nil
			}
			member := helpers.GetString(req.Arguments, "member")
			if member == "" {
				return helpers.ErrorResult("validation_error", "member is required for GEODIST"), nil
			}
			member2 := helpers.GetString(req.Arguments, "member2")
			if member2 == "" {
				return helpers.ErrorResult("validation_error", "member2 is required for GEODIST"), nil
			}
			unit := helpers.GetString(req.Arguments, "unit")
			if unit == "" {
				unit = "m"
			}
			dist, err := client.GeoDist(ctx, key, member, member2, unit).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			return helpers.TextResult(fmt.Sprintf("%.4f %s", dist, unit)), nil

		case "geopos":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for GEOPOS"), nil
			}
			members := helpers.GetStringSlice(req.Arguments, "members")
			if len(members) == 0 {
				// Fall back to single member param.
				member := helpers.GetString(req.Arguments, "member")
				if member == "" {
					return helpers.ErrorResult("validation_error", "members or member is required for GEOPOS"), nil
				}
				members = []string{member}
			}
			positions, err := client.GeoPos(ctx, key, members...).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			results := make([]map[string]any, len(members))
			for i, pos := range positions {
				entry := map[string]any{
					"member": members[i],
				}
				if pos != nil {
					entry["longitude"] = pos.Longitude
					entry["latitude"] = pos.Latitude
				} else {
					entry["longitude"] = nil
					entry["latitude"] = nil
				}
				results[i] = entry
			}
			resp, err := helpers.JSONResult(map[string]any{
				"positions": results,
			})
			if err != nil {
				return helpers.ErrorResult("json_error", err.Error()), nil
			}
			return resp, nil

		case "geosearch":
			if key == "" {
				return helpers.ErrorResult("validation_error", "key is required for GEOSEARCH"), nil
			}
			longitude := helpers.GetFloat64(req.Arguments, "longitude")
			latitude := helpers.GetFloat64(req.Arguments, "latitude")
			radius := helpers.GetFloat64(req.Arguments, "radius")
			if radius <= 0 {
				return helpers.ErrorResult("validation_error", "radius must be a positive number for GEOSEARCH"), nil
			}
			unit := helpers.GetString(req.Arguments, "unit")
			if unit == "" {
				unit = "m"
			}
			count := helpers.GetInt(req.Arguments, "count")

			query := &redis.GeoSearchLocationQuery{
				GeoSearchQuery: redis.GeoSearchQuery{
					Longitude:  longitude,
					Latitude:   latitude,
					Radius:     radius,
					RadiusUnit: unit,
					Sort:       "ASC",
				},
				WithDist:  true,
				WithCoord: true,
			}
			if count > 0 {
				query.GeoSearchQuery.Count = count
			}

			locations, err := client.GeoSearchLocation(ctx, key, query).Result()
			if err != nil {
				return helpers.ErrorResult("redis_error", err.Error()), nil
			}
			results := make([]map[string]any, len(locations))
			for i, loc := range locations {
				results[i] = map[string]any{
					"name":      loc.Name,
					"dist":      loc.Dist,
					"longitude": loc.Longitude,
					"latitude":  loc.Latitude,
				}
			}
			resp, err := helpers.JSONResult(map[string]any{
				"results": results,
				"count":   len(results),
			})
			if err != nil {
				return helpers.ErrorResult("json_error", err.Error()), nil
			}
			return resp, nil

		default:
			return helpers.ErrorResult("validation_error",
				fmt.Sprintf("unknown action %q (expected geoadd, geodist, geopos, or geosearch)", action)), nil
		}
	}
}

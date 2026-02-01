-- Create table for vehicle locations
CREATE TABLE IF NOT EXISTS vehicle_locations (
    id INTEGER PRIMARY KEY,
    trip_id VARCHAR(255) NOT NULL,
    occupancy_status INTEGER NOT NULL,
    bearing DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    route_id VARCHAR(255) NOT NULL,
    direction_id INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_trip_id ON vehicle_locations(trip_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_route_id ON vehicle_locations(route_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_timestamp ON vehicle_locations(timestamp);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_start_time ON vehicle_locations(start_time);

-- Optional: Create a spatial index if you'll be doing geographic queries
-- Requires PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
ALTER TABLE vehicle_locations ADD COLUMN location GEOGRAPHY(POINT, 4326);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_location ON vehicle_locations USING GIST(location);

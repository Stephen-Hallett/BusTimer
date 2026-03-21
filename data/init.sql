CREATE TABLE
    IF NOT EXISTS trips (
        trip_id VARCHAR(255) PRIMARY KEY,
        route_id VARCHAR(255) NOT NULL,
        service_id VARCHAR(255) NOT NULL,
        direction_id INTEGER NOT NULL,
        shape_id VARCHAR(255) NOT NULL,
        created_at TIMESTAMP
        WITH
            TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_trips_trip_id ON trips (trip_id);

CREATE INDEX IF NOT EXISTS idx_trips_route_id ON trips (route_id);

CREATE INDEX IF NOT EXISTS idx_trips_service_id ON trips (service_id);

-- Create table for vehicle locations
CREATE TABLE
    IF NOT EXISTS vehicle_locations (
        id INTEGER NOT NULL,
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
        schedule_relationship INTEGER NOT NULL,
        is_deleted BOOLEAN NOT NULL,
        stop_sequence INTEGER NOT NULL,
        stop_id VARCHAR(255) NOT NULL,
        stop_schedule_relationship INTEGER NOT NULL,
        departure_delay INTEGER,
        departure_time BIGINT,
        departure_uncertainty INTEGER,
        vehicle_id VARCHAR(255) NOT NULL,
        label VARCHAR(255) NOT NULL,
        license_plate VARCHAR(255),
        delay INTEGER NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id, timestamp)
    );

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_trip_id ON vehicle_locations (trip_id);

CREATE INDEX IF NOT EXISTS idx_vehicle_locations_route_id ON vehicle_locations (route_id);

CREATE INDEX IF NOT EXISTS idx_vehicle_locations_timestamp ON vehicle_locations (timestamp);

CREATE INDEX IF NOT EXISTS idx_vehicle_locations_start_time ON vehicle_locations (start_time);

-- Optional: Create a spatial index if you'll be doing geographic queries
-- Requires PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

ALTER TABLE vehicle_locations
ADD COLUMN location GEOGRAPHY(POINT, 4326) 
GENERATED ALWAYS AS (
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
) STORED;

-- Create spatial index
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_location ON vehicle_locations USING GIST (location);

-- Create table for trip-to-segment mappings
CREATE TABLE
    IF NOT EXISTS trip_segments (
        trip_id VARCHAR(255) NOT NULL,
        segment_id VARCHAR(255) NOT NULL,
        PRIMARY KEY (trip_id, segment_id)
    );

CREATE INDEX IF NOT EXISTS idx_trip_segments_trip_id ON trip_segments (trip_id);

CREATE INDEX IF NOT EXISTS idx_trip_segments_segment_id ON trip_segments (segment_id);

-- Create table for unique segment definitions
CREATE TABLE
    IF NOT EXISTS segments (
        segment_id VARCHAR(255) PRIMARY KEY,
        start_stop VARCHAR(255) NOT NULL,
        end_stop VARCHAR(255) NOT NULL,
        start_stop_id VARCHAR(255) NOT NULL,
        end_stop_id VARCHAR(255) NOT NULL,
        start_lat DOUBLE PRECISION NOT NULL,
        start_lon DOUBLE PRECISION NOT NULL,
        end_lat DOUBLE PRECISION NOT NULL,
        end_lon DOUBLE PRECISION NOT NULL
    );

CREATE TABLE   
    IF NOT EXISTS calendar (
        service_id VARCHAR(255) PRIMARY KEY,
        monday INTEGER NOT NULL,
        tuesday INTEGER NOT NULL,
        wednesday INTEGER NOT NULL,
        thursday INTEGER NOT NULL,
        friday INTEGER NOT NULL,
        saturday INTEGER NOT NULL,
        sunday INTEGER NOT NULL
    );

CREATE INDEX IF NOT EXISTS idx_calendar_service_id ON calendar (service_id);
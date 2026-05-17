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

CREATE TABLE
    IF NOT EXISTS stops (
        stop_id VARCHAR(255) PRIMARY KEY,
        location_type INTEGER NOT NULL,
        stop_code VARCHAR(255) NOT NULL,
        stop_lat DOUBLE PRECISION NOT NULL,
        stop_lon DOUBLE PRECISION NOT NULL,
        stop_name VARCHAR(255) NOT NULL
    );

CREATE TABLE
    IF NOT EXISTS segments (
        segment_id VARCHAR(255) PRIMARY KEY,
        start_stop_id VARCHAR(255) NOT NULL,
        end_stop_id VARCHAR(255) NOT NULL,
        CONSTRAINT fk_segments_start_stop FOREIGN KEY (start_stop_id) REFERENCES stops (stop_id) ON DELETE CASCADE,
        CONSTRAINT fk_segments_end_stop FOREIGN KEY (end_stop_id) REFERENCES stops (stop_id) ON DELETE CASCADE
    );

CREATE TABLE
    IF NOT EXISTS trips (
        trip_id VARCHAR(255) PRIMARY KEY,
        route_id VARCHAR(255) NOT NULL,
        service_id VARCHAR(255) NOT NULL,
        direction_id INTEGER NOT NULL,
        shape_id VARCHAR(255) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_trips_service_id FOREIGN KEY (service_id) REFERENCES calendar (service_id)
    );

CREATE INDEX IF NOT EXISTS idx_trips_trip_id ON trips (trip_id);
CREATE INDEX IF NOT EXISTS idx_trips_route_id ON trips (route_id);
CREATE INDEX IF NOT EXISTS idx_trips_service_id ON trips (service_id);

CREATE TABLE
    IF NOT EXISTS vehicle_locations (
        id INTEGER NOT NULL,
        trip_id VARCHAR(255) NOT NULL,
        occupancy_status INTEGER,
        bearing DOUBLE PRECISION,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        speed DOUBLE PRECISION NOT NULL,
        timestamp BIGINT NOT NULL,
        start_time TIMESTAMP WITH TIME ZONE NOT NULL,
        route_id VARCHAR(255) NOT NULL,
        direction_id INTEGER,
        schedule_relationship INTEGER,
        is_deleted BOOLEAN NOT NULL,
        stop_sequence INTEGER NOT NULL,
        stop_id VARCHAR(255) NOT NULL,
        stop_schedule_relationship INTEGER,
        departure_delay INTEGER,
        departure_time BIGINT,
        departure_uncertainty INTEGER,
        vehicle_id VARCHAR(255) NOT NULL,
        label VARCHAR(255) NOT NULL,
        license_plate VARCHAR(255),
        delay INTEGER NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id, timestamp),
        CONSTRAINT fk_vehicle_locations_trip_id FOREIGN KEY (trip_id) REFERENCES trips (trip_id)
    );

CREATE INDEX IF NOT EXISTS idx_vehicle_locations_trip_id ON vehicle_locations (trip_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_route_id ON vehicle_locations (route_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_timestamp ON vehicle_locations (timestamp);
CREATE INDEX IF NOT EXISTS idx_vehicle_locations_start_time ON vehicle_locations (start_time);

CREATE EXTENSION IF NOT EXISTS postgis;

ALTER TABLE vehicle_locations
ADD COLUMN location GEOGRAPHY(POINT, 4326)
GENERATED ALWAYS AS (
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
) STORED;

CREATE INDEX IF NOT EXISTS idx_vehicle_locations_location ON vehicle_locations USING GIST (location);

CREATE TABLE
    IF NOT EXISTS trip_segments (
        trip_id VARCHAR(255) NOT NULL,
        segment_id VARCHAR(255) NOT NULL,
        PRIMARY KEY (trip_id, segment_id),
        CONSTRAINT fk_trip_segments_trip_id FOREIGN KEY (trip_id) REFERENCES trips (trip_id) ON DELETE CASCADE,
        CONSTRAINT fk_trip_segments_segment_id FOREIGN KEY (segment_id) REFERENCES segments (segment_id) ON DELETE CASCADE
    );

CREATE INDEX IF NOT EXISTS idx_trip_segments_trip_id ON trip_segments (trip_id);
CREATE INDEX IF NOT EXISTS idx_trip_segments_segment_id ON trip_segments (segment_id);
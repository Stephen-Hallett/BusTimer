from geoalchemy2 import Geography
from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    Double,
    ForeignKey,
    Index,
    Integer,
    String,
    text,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Calendar(Base):
    __tablename__ = "calendar"

    service_id = Column(String(255), primary_key=True)
    monday = Column(Integer, nullable=False)
    tuesday = Column(Integer, nullable=False)
    wednesday = Column(Integer, nullable=False)
    thursday = Column(Integer, nullable=False)
    friday = Column(Integer, nullable=False)
    saturday = Column(Integer, nullable=False)
    sunday = Column(Integer, nullable=False)

    trips = relationship("Trip", back_populates="calendar")

    __table_args__ = (Index("idx_calendar_service_id", "service_id"),)


class Stop(Base):
    __tablename__ = "stops"
    stop_id = Column(String(255), primary_key=True)
    location_type = Column(Integer, nullable=False)
    stop_code = Column(String(255), nullable=False)
    stop_lat = Column(Double, nullable=False)
    stop_lon = Column(Double, nullable=False)
    stop_name = Column(String(255), nullable=False)


class Segment(Base):
    __tablename__ = "segments"

    segment_id = Column(String(255), primary_key=True)
    start_stop_id = Column(String(255), ForeignKey("stops.stop_id"), nullable=False)
    end_stop_id = Column(String(255), ForeignKey("stops.stop_id"), nullable=False)

    trip_segments = relationship("TripSegment", back_populates="segment")


class Trip(Base):
    __tablename__ = "trips"

    trip_id = Column(String(255), primary_key=True)
    route_id = Column(String(255), nullable=False)
    service_id = Column(String(255), ForeignKey("calendar.service_id"), nullable=False)
    direction_id = Column(Integer, nullable=False)
    shape_id = Column(String(255), nullable=False)
    created_at = Column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP")
    )

    calendar = relationship("Calendar", back_populates="trips")
    vehicle_locations = relationship("VehicleLocation", back_populates="trip")
    trip_segments = relationship("TripSegment", back_populates="trip")

    __table_args__ = (
        Index("idx_trips_trip_id", "trip_id"),
        Index("idx_trips_route_id", "route_id"),
        Index("idx_trips_service_id", "service_id"),
    )


class VehicleLocation(Base):
    __tablename__ = "vehicle_locations"

    id = Column(Integer, nullable=False, primary_key=True)
    timestamp = Column(BigInteger, nullable=False, primary_key=True)
    trip_id = Column(String(255), ForeignKey("trips.trip_id"), nullable=False)
    occupancy_status = Column(Integer, nullable=True)
    bearing = Column(Double, nullable=True)
    latitude = Column(Double, nullable=False)
    longitude = Column(Double, nullable=False)
    speed = Column(Double, nullable=False)
    start_time = Column(TIMESTAMP(timezone=True), nullable=False)
    route_id = Column(String(255), nullable=False)
    direction_id = Column(Integer, nullable=True)
    schedule_relationship = Column(Integer, nullable=True)
    is_deleted = Column(Boolean, nullable=False)
    stop_sequence = Column(Integer, nullable=False)
    stop_id = Column(String(255), ForeignKey("stops.stop_id"), nullable=False)
    stop_schedule_relationship = Column(Integer, nullable=True)
    departure_delay = Column(Integer, nullable=True)
    departure_time = Column(BigInteger, nullable=True)
    departure_uncertainty = Column(Integer, nullable=True)
    vehicle_id = Column(String(255), nullable=False)
    label = Column(String(255), nullable=False)
    license_plate = Column(String(255), nullable=True)
    delay = Column(Integer, nullable=False)
    created_at = Column(
        TIMESTAMP(timezone=True), server_default=text("CURRENT_TIMESTAMP")
    )
    location = Column(
        Geography(geometry_type="POINT", srid=4326),
        nullable=True,
        comment="Generated column — managed by Postgres, not SQLAlchemy",
    )

    trip = relationship("Trip", back_populates="vehicle_locations")

    __table_args__ = (
        Index("idx_vehicle_locations_trip_id", "trip_id"),
        Index("idx_vehicle_locations_route_id", "route_id"),
        Index("idx_vehicle_locations_timestamp", "timestamp"),
        Index("idx_vehicle_locations_start_time", "start_time"),
        Index("idx_vehicle_locations_location", "location", postgresql_using="gist"),
    )


class TripSegment(Base):
    __tablename__ = "trip_segments"

    trip_id = Column(String(255), ForeignKey("trips.trip_id"), primary_key=True)
    segment_id = Column(
        String(255), ForeignKey("segments.segment_id"), primary_key=True
    )

    trip = relationship("Trip", back_populates="trip_segments")
    segment = relationship("Segment", back_populates="trip_segments")

    __table_args__ = (
        Index("idx_trip_segments_trip_id", "trip_id"),
        Index("idx_trip_segments_segment_id", "segment_id"),
    )

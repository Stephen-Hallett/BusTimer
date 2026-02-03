from pydantic import BaseModel


class Trip(BaseModel):
    trip_id: str
    route_id: str
    service_id: str
    direction_id: int
    shape_id: str

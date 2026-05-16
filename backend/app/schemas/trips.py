from pydantic import BaseModel, ConfigDict


class Trip(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    trip_id: str
    route_id: str
    service_id: str
    direction_id: int
    shape_id: str

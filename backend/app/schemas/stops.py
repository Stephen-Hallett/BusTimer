from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

Latitude = Annotated[float, Field(ge=-90.0, le=90.0)]
Longitude = Annotated[float, Field(ge=-180.0, le=180.0)]


class Stop(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    stop_id: str
    location_type: int
    stop_code: str
    stop_lat: Latitude
    stop_lon: Longitude
    stop_name: str

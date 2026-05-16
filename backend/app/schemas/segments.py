from pydantic import BaseModel, ConfigDict

class Segment(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    start_stop_id: str
    end_stop_id: str

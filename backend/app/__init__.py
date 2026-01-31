from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .router.vehicles import router as VehicleRouter
from .utils.logger import MyLogger

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = MyLogger().get_logger()

app.include_router(VehicleRouter, prefix="/vehicles")


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "healthy"}

from fastapi import FastAPI

from .routers import features, health, tiles

app = FastAPI()
app.include_router(features.router)
app.include_router(health.router)
app.include_router(tiles.router)

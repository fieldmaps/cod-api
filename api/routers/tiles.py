from os import getenv

from fastapi import APIRouter
from fastapi.responses import RedirectResponse

TILES_URL = getenv("TILES_URL", "")
S3_ASSETS_URL = getenv("S3_ASSETS_URL", "")

router = APIRouter()


@router.get(
    "/tiles/{processing_level}/{iso3}/{admin_level}",
    description="Tiles",
    tags=["vectors"],
)
async def tiles(processing_level: int, iso3: str, admin_level: int) -> RedirectResponse:
    """Convert features to other file format.

    Returns:
        Converted File.
    """
    layer = f"{iso3}_adm{admin_level}".lower()
    asset_url = f"{TILES_URL}/level-{processing_level}/{layer}.json"
    return RedirectResponse(asset_url)


@router.get(
    "/images/{processing_level}/{iso3}/{admin_level}",
    description="Images",
    tags=["vectors"],
)
async def images(
    processing_level: int,
    iso3: str,
    admin_level: int,
) -> RedirectResponse:
    """Convert features to other file format.

    Returns:
        Converted File.
    """
    layer = f"{iso3}_adm{admin_level}".lower()
    asset_url = f"{S3_ASSETS_URL}/level-{processing_level}/{layer}.webp"
    return RedirectResponse(asset_url)

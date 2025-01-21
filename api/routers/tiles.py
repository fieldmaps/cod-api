from fastapi import APIRouter
from fastapi.responses import RedirectResponse

TILES_URL = "https://cod-tiles.fieldmaps.io/assets"

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

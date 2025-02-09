from os import getenv
from uuid import uuid4

# from typing import Annotated
# from fastapi import APIRouter, HTTPException, Query, status
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import RedirectResponse
from httpx import AsyncClient, codes

GDAL_URL = getenv("GDAL_URL", "")
S3_ASSETS_URL = getenv("S3_ASSETS_URL", "")
S3_CACHE_URL = getenv("S3_CACHE_URL", "")

router = APIRouter()


def get_remote_format(remote_format: str) -> str:
    """Add .zip to formats outputing to directory."""
    if remote_format in ["shp", "gdb"]:
        return f"{remote_format}.zip"
    return remote_format


@router.get(
    "/features/{processing_level}/{iso3}/{admin_level}",
    description="Get vector in any GDAL/OGR supported format",
    tags=["vectors"],
    response_class=RedirectResponse,
    status_code=status.HTTP_308_PERMANENT_REDIRECT,
)
async def features(
    processing_level: str,
    iso3: str,
    admin_level: int,
    f: str = "geojson",
    # simplify: str | None = None,
    # lco: Annotated[list[str] | None, Query()] = None,
) -> str:
    """Convert features to other file format.

    Returns:
        Converted File.
    """
    f = f.lower().lstrip(".")
    processing_level = processing_level.lower()
    layer = f"{iso3}_adm{admin_level}".lower()
    assets_url = f"{S3_ASSETS_URL}/level-{processing_level}/{layer}.parquet"
    if f == "parquet":
        return assets_url
    remote_format = get_remote_format(f)
    cache_url = f"{S3_CACHE_URL}/level-{processing_level}/{layer}.{remote_format}"
    async with AsyncClient() as client:
        r1 = await client.head(cache_url + f"?v={uuid4()}")
    if r1.status_code == codes.OK:
        return cache_url
    # lco_options = [("-lco", x) for x in lco] if lco is not None else []
    # simplify_options = ["-simplify", simplify] if simplify is not None else []
    async with AsyncClient() as client:
        r2 = await client.get(
            f"{GDAL_URL}/ogr2ogr/{processing_level}/{iso3}/{admin_level}?f={f}",
        )
    if r2.status_code == codes.OK:
        return cache_url + f"?v={uuid4()}"
    if r2.status_code >= codes.BAD_REQUEST:
        raise HTTPException(
            status_code=r2.status_code,
            detail=r2.content,
        )
    return ""

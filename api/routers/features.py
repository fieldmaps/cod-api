from asyncio import create_subprocess_exec
from os import getenv
from pathlib import Path
from shutil import make_archive
from tempfile import TemporaryDirectory
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import RedirectResponse
from httpx import AsyncClient, codes

S3_ASSETS_URL = getenv("S3_ASSETS_URL", "")
S3_CACHE_URL = getenv("S3_CACHE_URL", "")
S3_CACHE_BUCKET = getenv("S3_CACHE_BUCKET", "")

router = APIRouter()


def get_options(output_format: str) -> list[str]:
    """Get recommended options for output formats.

    Args:
        output_format: suffix of the output file

    Returns:
        List of layer creation options for each file type
    """
    match output_format:
        case "shp.zip":
            return ["-lco", "ENCODING=UTF-8"]
        case _:
            return []


def get_format(output_format: str) -> str:
    """Add .zip to formats outputing to directory.

    Args:
        output_format: suffix of the output file

    Returns:
        List of layer creation options for each file type
    """
    if output_format in ["shp", "gdb"]:
        return f"{output_format}.zip"
    return output_format


@router.get(
    "/{processing_level}/{iso3}/{admin_level}/features",
    description="Get vector in any GDAL/OGR supported format",
    tags=["vectors"],
    response_class=RedirectResponse,
    status_code=status.HTTP_308_PERMANENT_REDIRECT,
)
async def features(  # noqa: PLR0913
    processing_level: int,
    iso3: str,
    admin_level: int,
    f: str = "geojson",
    simplify: str | None = None,
    lco: Annotated[list[str] | None, Query()] = None,
) -> str:
    """Convert features to other file format.

    Returns:
        Converted File.
    """
    f = f.lower()
    layer = f"{iso3}_adm{admin_level}".lower()
    asset_url = f"{S3_ASSETS_URL}/level-{processing_level}/{layer}.parquet"
    if f == "parquet":
        return asset_url
    cache_url = f"{S3_CACHE_URL}/level-{processing_level}/{layer}.{get_format(f)}"
    cache_bucket = f"{S3_CACHE_BUCKET}/level-{processing_level}/{layer}.{get_format(f)}"
    async with AsyncClient() as client:
        response = await client.head(cache_url)
    if response.status_code == codes.OK:
        return cache_url
    of = f if f != "shp" else "shp.zip"
    options = get_options(of)
    lco_options = [("-lco", x) for x in lco] if lco is not None else []
    simplify_options = ["-simplify", simplify] if simplify is not None else []
    with TemporaryDirectory() as tmp:
        output = Path(tmp) / f"{layer}.{of}"
        ogr2ogr = await create_subprocess_exec(
            "ogr2ogr",
            "-overwrite",
            *["--config", "GDAL_NUM_THREADS", "ALL_CPUS"],
            *["--config", "OGR_GEOJSON_MAX_OBJ_SIZE", "0"],
            *["-nln", layer],
            *simplify_options,
            *[x for y in lco_options for x in y],
            *options,
            output,
            asset_url,
        )
        await ogr2ogr.wait()
        if output.stat().st_size == 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Unprocessable Content",
            )
        if output.is_dir():
            make_archive(str(output), "zip", output)
            output = output.with_suffix(f".{f}.zip")
        rclone = await create_subprocess_exec("rclone", "copyto", output, cache_bucket)
        await rclone.wait()
    return cache_url

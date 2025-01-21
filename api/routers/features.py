from asyncio import create_subprocess_exec
from os import getenv
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

# from typing import Annotated
# from fastapi import APIRouter, HTTPException, Query, status
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import RedirectResponse
from httpx import AsyncClient, codes

S3_ASSETS_URL = getenv("S3_ASSETS_URL", "")
S3_CACHE_URL = getenv("S3_CACHE_URL", "")
S3_ASSETS_BUCKET = getenv("S3_ASSETS_BUCKET", "")
S3_CACHE_BUCKET = getenv("S3_CACHE_BUCKET", "")
S3_CHUNK_SIZE = getenv("S3_CHUNK_SIZE", "")

router = APIRouter()


def get_recommended_options(local_format: str) -> list[str]:
    """Get recommended options for output formats.

    Args:
        local_format: suffix of the output file

    Returns:
        List of layer creation options for each file type
    """
    match local_format:
        case "shp.zip":
            return ["-lco", "ENCODING=UTF-8"]
        case _:
            return []


def get_local_format(local_format: str) -> str:
    """Remove .zip from formats outputing to directory and add .zip to shapefiles.

    Args:
        local_format: suffix of the input file

    Returns:
        Cleaned suffix
    """
    if local_format == "gdb.zip":
        return local_format.rstrip(".zip")
    if local_format == "shp":
        return f"{local_format}.zip"
    return local_format


def get_remote_format(remote_format: str) -> str:
    """Add .zip to formats outputing to directory.

    Args:
        remote_format: suffix of the output file

    Returns:
        Cleaned suffix
    """
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
    processing_level: int,
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
    layer = f"{iso3}_adm{admin_level}".lower()
    assets_url = f"{S3_ASSETS_URL}/level-{processing_level}/{layer}.parquet"
    assets_bucket = f"{S3_ASSETS_BUCKET}/level-{processing_level}/{layer}.parquet"
    if f == "parquet":
        return assets_url
    local_format = get_local_format(f)
    remote_format = get_remote_format(f)
    cache_url = f"{S3_CACHE_URL}/level-{processing_level}/{layer}.{remote_format}"
    cache_bucket = f"{S3_CACHE_BUCKET}/level-{processing_level}/{layer}.{remote_format}"
    async with AsyncClient() as client:
        response = await client.head(cache_url + f"?v={uuid4()}")
    if response.status_code == codes.OK:
        return cache_url
    recommended_options = get_recommended_options(local_format)
    # lco_options = [("-lco", x) for x in lco] if lco is not None else []
    # simplify_options = ["-simplify", simplify] if simplify is not None else []
    with TemporaryDirectory() as tmp:
        input_path = Path(tmp) / f"{layer}.parquet"
        output_path = Path(tmp) / f"{layer}.{local_format}"
        rclone_download = await create_subprocess_exec(
            "rclone",
            "copyto",
            *["--s3-chunk-size", S3_CHUNK_SIZE],
            assets_bucket,
            input_path,
        )
        await rclone_download.wait()
        ogr2ogr = await create_subprocess_exec(
            "ogr2ogr",
            "-overwrite",
            *["--config", "GDAL_NUM_THREADS", "ALL_CPUS"],
            *["--config", "OGR_GEOJSON_MAX_OBJ_SIZE", "0"],
            *["--config", "OGR_ORGANIZE_POLYGONS", "ONLY_CCW"],
            *["-nln", layer],
            # *simplify_options,
            # *[x for y in lco_options for x in y],
            *recommended_options,
            output_path,
            assets_url,
        )
        await ogr2ogr.wait()
        if output_path.stat().st_size == 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Unprocessable Content",
            )
        if output_path.is_dir():
            output_zip = output_path.with_suffix(f".{f}.zip")
            sozip = await create_subprocess_exec(
                "sozip",
                "-r",
                output_zip,
                output_path,
            )
            await sozip.wait()
            output_path = output_zip
        rclone_upload = await create_subprocess_exec(
            "rclone",
            "copyto",
            *["--s3-chunk-size", S3_CHUNK_SIZE],
            output_path,
            cache_bucket,
        )
        await rclone_upload.wait()
    return cache_url + f"?v={uuid4()}"

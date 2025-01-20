import mimetypes
from asyncio import create_subprocess_exec
from pathlib import Path
from shutil import make_archive
from tempfile import TemporaryDirectory
from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse

DATA_URL = "https://cod-data.fieldmaps.io/assets"

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
        case "geojson":
            return ["-lco", "RFC7946=YES"]
        case _:
            return []


@router.get(
    "/{processing_level}/{iso3}/{admin_level}/features",
    description="Get vector in any GDAL/OGR supported format",
    tags=["vectors"],
)
async def features(  # noqa: PLR0913
    processing_level: int,
    iso3: str,
    admin_level: int,
    f: str = "geojson",
    simplify: str | None = None,
    lco: Annotated[list[str] | None, Query()] = None,
) -> FileResponse:
    """Convert features to other file format.

    Returns:
        Converted File.
    """
    layer = f"{iso3}_adm{admin_level}".lower()
    asset_url = f"{DATA_URL}/level-{processing_level}/{layer}.parquet"
    f = f if f != "shp" else "shp.zip"
    options = get_options(f)
    lco_options = [("-lco", x) for x in lco] if lco is not None else []
    simplify_options = ["-simplify", simplify] if simplify is not None else []
    with TemporaryDirectory(delete=False) as tmp:
        output = Path(tmp) / f"{layer}.{f}"
        process = await create_subprocess_exec(
            "ogr2ogr",
            *[
                "-overwrite",
                *["--config", "GDAL_NUM_THREADS", "ALL_CPUS"],
                *["--config", "OGR_GEOJSON_MAX_OBJ_SIZE", "0"],
                *["-nln", layer],
                *simplify_options,
                *[x for y in lco_options for x in y],
                *options,
                output,
                asset_url,
            ],
        )
        await process.wait()
        if output.is_dir():
            make_archive(str(output), "zip", output)
            output = output.with_suffix(f".{f}.zip")
        media_type, _ = mimetypes.guess_type(output)
        return FileResponse(output, filename=output.name, media_type=media_type)

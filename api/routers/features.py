import mimetypes
from pathlib import Path
from shutil import make_archive
from subprocess import DEVNULL, run
from tempfile import TemporaryDirectory
from typing import Annotated

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse

DATA_URL = "https://cod-data.fieldmaps.io/assets"

router = APIRouter()
mimetypes.add_type("application/geo+json", ".geojson")
mimetypes.add_type("application/geopackage+sqlite3", ".gpkg")


def get_options(output_format: str):
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
def features(
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
        run(
            [
                "ogr2ogr",
                "-overwrite",
                *["-nln", layer],
                *simplify_options,
                *[x for y in lco_options for x in y],
                *options,
                output,
                asset_url,
            ],
            stderr=DEVNULL,
            check=False,
        )
        if output.is_dir():
            make_archive(str(output), "zip", output)
            output = output.with_suffix(f".{f}.zip")
        media_type, _ = mimetypes.guess_type(output)
        return FileResponse(output, filename=output.name, media_type=media_type)

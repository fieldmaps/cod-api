import re
from datetime import UTC, datetime, time
from os import getenv
from shutil import rmtree

import pystac
from geopandas import GeoDataFrame, GeoSeries, read_parquet
from pystac.extensions.projection import ProjectionExtension
from tqdm import tqdm

from .config import l1, stac
from .utils import get_metadata

API_URL = getenv("API_URL", "")
S3_ASSETS_URL = getenv("S3_ASSETS_URL", "")
TILES_URL = getenv("TILES_URL", "")

formats = [
    ("geojson", pystac.MediaType.GEOJSON),
    ("gpkg", pystac.MediaType.GEOPACKAGE),
    ("gpkg.zip", pystac.MediaType.GEOPACKAGE),
    ("kml", pystac.MediaType.KML),
    ("fgb", pystac.MediaType.FLATGEOBUF),
    ("shp.zip", "application/x-shapefile"),
    ("gdb.zip", "application/x-filegdb"),
]


def get_date(gdf: GeoDataFrame, key: str):
    return (
        datetime.combine(gdf[key].iloc[0], time(0, 0, 0))
        if key in gdf
        else datetime.now(tz=UTC)
    )


def get_langs(gdf: GeoDataFrame, admin_level: int | str | None = None) -> list[str]:
    """Gets a list of language codes.

    Args:
        gdf: Current layer's GeoDataFrame.
        admin_level: Current layer's admin level.

    Returns:
        _description_
    """
    columns = list(gdf.columns)
    if admin_level is None:
        p = re.compile(r"^ADM\d_\w{2}$")
    else:
        p = re.compile(rf"^ADM{admin_level}_\w{{2}}$")
    langs = [x.split("_")[1].lower() for x in columns if p.search(x)]
    return list(dict.fromkeys(langs))


def main() -> None:
    """Main function, runs all modules in sequence."""
    metadata_all = get_metadata()
    catalog = pystac.Catalog(
        id="cod-ab",
        description=(
            "Catalog for Common Operational Datasets - Administrative Boundaries "
            "(COD-AB)."
        ),
    )
    geometries = []
    intervals = []
    items = []
    files = sorted(l1.glob("*.parquet"))
    pbar = tqdm(files)
    for file in pbar:
        pbar.set_postfix_str(file.stem)
        iso3, adm_level = file.stem.split("_adm")
        metadata = metadata_all.get(iso3.upper(), {})
        gdf = read_parquet(file)
        dissolve = gdf.dissolve()
        item = pystac.Item(
            id=file.stem,
            geometry=dissolve.convex_hull.iloc[0].__geo_interface__,
            bbox=dissolve.total_bounds.tolist(),
            start_datetime=get_date(gdf, "date"),
            end_datetime=get_date(gdf, "validOn"),
            datetime=get_date(gdf, "validOn"),
            properties={
                "admin_level": adm_level,
                "languages_admin_self": get_langs(gdf, adm_level),
                "languages_admin_all": get_langs(gdf),
                **metadata,
            },
        )
        proj_ext = ProjectionExtension.ext(item, add_if_missing=True)
        proj_ext.epsg = gdf.geometry.crs.to_epsg() or 4326
        item.add_asset(
            key="parquet",
            asset=pystac.Asset(
                href=f"{S3_ASSETS_URL}/level-1/{iso3}_adm{adm_level}.parquet",
                media_type="application/vnd.apache.parquet",
                roles=["data"],
            ),
        )
        for key, media in formats:
            item.add_asset(
                key=key,
                asset=pystac.Asset(
                    href=f"{API_URL}/features/1/{iso3}/{adm_level}?f={key}",
                    media_type=media,
                    roles=["data"],
                ),
            )
        item.add_asset(
            key="tilejson",
            asset=pystac.Asset(
                href=f"{TILES_URL}/level-1/{iso3}_adm{adm_level}.json",
                media_type=pystac.MediaType.JSON,
                roles=["data"],
            ),
        )
        item.add_asset(
            key="pmtiles",
            asset=pystac.Asset(
                href=f"{S3_ASSETS_URL}/level-1/{iso3}_adm{adm_level}.pmtiles",
                media_type="application/vnd.pmtiles",
                roles=["data"],
            ),
        )
        item.add_asset(
            key="webp",
            asset=pystac.Asset(
                href=f"{S3_ASSETS_URL}/level-1/{iso3}_adm{adm_level}.webp",
                media_type="image/webp",
                roles=["thumbnail", "overview"],
            ),
        )
        geometries.append(dissolve.geometry.envelope.iloc[0])
        intervals.append(item.datetime)
        items.append(item)
    collection = pystac.Collection(
        id="cod-ab-1",
        description="COD-AB at level-1 processing.",
        extent=pystac.Extent(
            pystac.SpatialExtent(GeoSeries(geometries).total_bounds.tolist()),
            pystac.TemporalExtent(intervals),
        ),
        license="CC-BY-3.0-IGO",
    )
    collection.add_items(items)
    catalog.add_child(collection)
    rmtree(stac, ignore_errors=True)
    catalog.normalize_and_save(str(stac), pystac.CatalogType.SELF_CONTAINED)


if __name__ == "__main__":
    main()

from datetime import UTC, datetime, time
from os import getenv
from shutil import rmtree

from geopandas import GeoSeries, read_parquet
from pystac import (
    Asset,
    Catalog,
    CatalogType,
    Collection,
    Extent,
    Item,
    MediaType,
    SpatialExtent,
    TemporalExtent,
)
from tqdm import tqdm

from .config import l1, stac

API_URL = getenv("API_URL", "")

formats = [
    ("geojson", MediaType.GEOJSON),
    ("parquet", MediaType.PARQUET),
    ("gpkg", MediaType.GEOPACKAGE),
    ("gpkg.zip", "application/zip"),
    ("kml", MediaType.KML),
    ("fgb", MediaType.FLATGEOBUF),
    ("shp.zip", "application/zip"),
    ("gdb.zip", "application/zip"),
]


def main() -> None:
    """Main function, runs all modules in sequence."""
    catalog = Catalog(
        id="cod-catalog",
        description="Catalogue for Common Operational Datasets.",
    )
    geometries = []
    intervals = []
    items = []
    files = sorted(l1.glob("*.parquet"))[0:20]
    pbar = tqdm(files)
    for file in pbar:
        pbar.set_postfix_str(file.stem)
        iso3, adm_level = file.stem.split("_adm")
        gdf = read_parquet(file)
        dissolve = gdf.dissolve()
        item = Item(
            id=file.stem,
            geometry=dissolve.convex_hull.iloc[0].__geo_interface__,
            bbox=dissolve.total_bounds.tolist(),
            datetime=(
                datetime.combine(gdf["validOn"].iloc[0], time(0, 0, 0))
                if "validOn" in gdf
                else datetime.now(tz=UTC)
            ),
            properties={},
        )
        for key, media in formats:
            item.add_asset(
                key=key,
                asset=Asset(
                    href=f"{API_URL}/features/1/{iso3}/{adm_level}?f={key}",
                    media_type=media,
                ),
            )
        geometries.append(dissolve.geometry.envelope.iloc[0])
        intervals.append(item.datetime)
        items.append(item)
    collection = Collection(
        id="cods",
        description="Current CODs.",
        extent=Extent(
            SpatialExtent(GeoSeries(geometries).total_bounds.tolist()),
            TemporalExtent(intervals),
        ),
        license="CC-BY-3.0-IGO",
    )
    collection.add_items(items)
    catalog.add_child(collection)
    rmtree(stac, ignore_errors=True)
    catalog.normalize_and_save(str(stac), CatalogType.SELF_CONTAINED)


if __name__ == "__main__":
    main()

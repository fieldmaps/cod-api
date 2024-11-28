from datetime import UTC, datetime
from shutil import rmtree

from geopandas import GeoSeries, read_file
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

from .config import inputs, stac


def main() -> None:
    """Main function, runs all modules in sequence."""
    catalog = Catalog(
        id="cod-catalog",
        description="Catalogue for Common Operational Datasets.",
    )
    geometries = []
    intervals = []
    items = []
    files = sorted(inputs.glob("*.gpkg"))
    pbar = tqdm(files)
    for file in pbar:
        pbar.set_postfix_str(file.stem)
        gdf = read_file(file, use_arrow=True)
        dissolve = gdf.dissolve()
        item = Item(
            id=file.stem,
            geometry=dissolve.convex_hull.iloc[0].__geo_interface__,
            bbox=dissolve.total_bounds.tolist(),
            datetime=(
                gdf["validOn"].iloc[0] if "validOn" in gdf else datetime.now(tz=UTC)
            ),
            properties={},
        )
        item.add_asset(
            key="geojson",
            asset=Asset(
                href="https://cod-data.fieldmaps.io",
                media_type=MediaType.GEOJSON,
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

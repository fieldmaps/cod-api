import re
from datetime import UTC, datetime, time
from os import getenv
from shutil import rmtree

import httpx
import pystac
from geopandas import GeoDataFrame, GeoSeries, read_parquet
from numpy import nan
from pycountry import countries, languages
from shapely.geometry import box
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
    ("gdb.zip", "application/x-filegdb"),
    ("shp.zip", "application/x-shapefile"),
    ("kml", pystac.MediaType.KML),
    ("fgb", pystac.MediaType.FLATGEOBUF),
]


def get_date(gdf: GeoDataFrame, key: str):
    return (
        datetime.combine(gdf[key].iloc[0], time(0, 0, 0))
        if key in gdf
        else datetime.now(tz=UTC)
    )


def get_langs(gdf: GeoDataFrame, admin_level: int | None = None) -> list[dict]:
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
    langs = list(dict.fromkeys(langs))
    return [{"code": x, "name": languages.get(alpha_2=x).name} for x in langs]


def add_assets(item: pystac.Item, iso3: str, adm_level: int) -> pystac.Item:
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
    item.add_link(
        pystac.Link(
            rel=pystac.RelType.PREVIEW,
            target=f"{S3_ASSETS_URL}/level-1/{iso3.lower()}_adm{adm_level}.webp",
            media_type="image/webp",
        ),
    )
    return item


def main() -> None:
    """Main function, runs all modules in sequence."""
    metadata_all = get_metadata()
    catalog = pystac.Catalog(
        id="cod-ab",
        title="COD-AB",
        description="Common Operational Datasets - Administrative Boundaries.",
    )
    collections = []
    geometries_all = []
    intervals_all = []
    pbar = tqdm(metadata_all)
    for metadata in pbar:
        iso3 = metadata["iso3"]
        pbar.set_postfix_str(iso3)
        r = httpx.get(
            f"https://data.humdata.org/api/3/action/package_show?id=cod-ab-{iso3.lower()}",
        )
        hdx = r.json()["result"]
        files = sorted(l1.glob(f"{iso3.lower()}_adm*.parquet"))
        if len(files) == 0:
            continue
        items = []
        geometries = []
        intervals = []
        proj_codes = set()
        for file in files:
            adm_level = int(file.stem.split("_adm")[1])
            gdf = read_parquet(file)
            # dissolve = gdf.dissolve()
            country = countries.get(alpha_3=iso3)
            item = pystac.Item(
                id=file.stem,
                # geometry=dissolve.convex_hull.iloc[0].__geo_interface__,
                # bbox=dissolve.total_bounds.tolist(),
                geometry=box(*gdf.total_bounds).__geo_interface__,
                bbox=gdf.total_bounds.tolist(),
                start_datetime=get_date(gdf, "date"),
                end_datetime=get_date(gdf, "validOn"),
                datetime=get_date(gdf, "validOn"),
                properties={
                    "admin:level": adm_level,
                    "admin:count": len(gdf.index),
                    "languages": get_langs(gdf, adm_level),
                    "proj:code": f"EPSG:{gdf.geometry.crs.to_epsg() or 4326}",
                },
            )
            item = add_assets(item, iso3.lower(), adm_level)
            if (
                metadata["itos_url"] is not nan
                and metadata[f"itos_index_{adm_level}"] >= 0
            ):
                item.add_link(
                    pystac.Link(
                        rel=pystac.RelType.VIA,
                        target=(
                            f"{metadata['itos_url']}/"
                            f"{int(metadata[f'itos_index_{adm_level}'])}/query"
                            "?f=json&where=1=1&outFields=*&orderByFields=OBJECTID"
                        ),
                        title="ITOS ArcGIS Feature Server ESRI JSON",
                    ),
                )
            items.append(item)
            # geometries.append(dissolve.geometry.envelope.iloc[0])
            # geometries_all.append(dissolve.geometry.envelope.iloc[0])
            proj_codes.add(f"EPSG:{gdf.geometry.crs.to_epsg() or 4326}")
            geometries.append(gdf.geometry.envelope.iloc[0])
            geometries_all.append(gdf.geometry.envelope.iloc[0])
            intervals.append(get_date(gdf, "date"))
            intervals.append(get_date(gdf, "validOn"))
            intervals_all.append(item.datetime)
        collection = pystac.Collection(
            id=f"cod-ab-l1-{iso3.lower()}",
            title=metadata["name"],
            description=f"COD-AB at Level-1 processing for {metadata['name']}.",
            extent=pystac.Extent(
                pystac.SpatialExtent(GeoSeries(geometries).total_bounds.tolist()),
                pystac.TemporalExtent([sorted(intervals)[i] for i in (0, -1)]),
            ),
            license="CC-BY-3.0-IGO",
            summaries=pystac.Summaries(
                {
                    "languages": get_langs(gdf),
                    "country:alpha_3": iso3,
                    "country:alpha_2": country.alpha_2,
                    "country:numeric": country.numeric,
                    "hdx:notes": hdx["notes"],
                    "hdx:dataset_source": hdx["dataset_source"],
                    "hdx:organization": hdx["organization"]["name"],
                    "hdx:methodology": hdx["methodology"],
                    "hdx:methodology_other": hdx.get("methodology_other"),
                    "hdx:caveats": hdx.get("caveats"),
                },
            ),
        )
        if len(proj_codes) == 1:
            collection.summaries.add("proj:code", next(iter(proj_codes)))
        collection.add_link(
            pystac.Link(
                rel=pystac.RelType.PREVIEW,
                target=f"{S3_ASSETS_URL}/level-1/{iso3.lower()}_adm{adm_level}.webp",
                media_type="image/webp",
            ),
        )
        collection.add_link(
            pystac.Link(
                rel=pystac.RelType.VIA,
                target=metadata["hdx_url"],
                title="HDX Dataset Page",
            ),
        )
        if metadata["itos_url"] is not nan:
            collection.add_link(
                pystac.Link(
                    rel=pystac.RelType.VIA,
                    target=metadata["itos_url"],
                    title="ITOS ArcGIS Feature Server",
                ),
            )
        collection.add_items(items)
        collections.append(collection)
    collection_all = pystac.Collection(
        id="cod-ab-l1",
        title="Level-1 Processing",
        description=(
            "Geometry, topology, character encoding and schema cleaning applied."
        ),
        extent=pystac.Extent(
            pystac.SpatialExtent(GeoSeries(geometries_all).total_bounds.tolist()),
            pystac.TemporalExtent([sorted(intervals_all)[i] for i in (0, -1)]),
        ),
        license="CC-BY-3.0-IGO",
    )
    collection_all.add_children(collections)
    catalog.add_child(collection_all)
    rmtree(stac, ignore_errors=True)
    catalog.normalize_and_save(str(stac), pystac.CatalogType.SELF_CONTAINED)


if __name__ == "__main__":
    main()

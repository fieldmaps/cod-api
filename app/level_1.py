from re import match

from geopandas import GeoDataFrame
from pandas import NaT, Timestamp
from pycountry import countries
from tqdm import tqdm

from .config import (
    ADMIN_LEVEL_MAX,
    WGS84,
    apostrophe_chars,
    invisible_chars,
    level_1_fixes,
)
from .utils import get_adm0_name, get_epsg_ease, read_parquet, to_parquet


def dissolve_and_save(gdf: GeoDataFrame, iso3: str, admin_levels: int):
    for admin_level in range(admin_levels, -1, -1):
        columns = []
        for level in range(admin_level, -1, -1):
            columns += [
                column
                for column in gdf.columns
                if match(rf"^ADM{level}_[A-Z][A-Z]$", column)
            ]
            columns += [f"ADM{level}_PCODE"]
        columns += ["date", "validOn", "validTo", "AREA_SQKM", "geometry"]
        gdf = gdf.dissolve(f"ADM{admin_level}_PCODE", as_index=False)
        _, min_y, _, max_y = gdf.geometry.total_bounds
        epsg_ease = get_epsg_ease(min_y, max_y)
        gdf["AREA_SQKM"] = gdf.geometry.to_crs(epsg_ease).area / 1e6
        gdf = gdf[columns]
        gdf = gdf.sort_values(by=[f"ADM{admin_level}_PCODE"])
        to_parquet(gdf, iso3, admin_level, 1)


def name_fixes(gdf: GeoDataFrame, iso3: str, iso2: str, admin_level: int):
    name_columns = [
        column
        for level in range(admin_level, -1, -1)
        for column in gdf.columns
        if match(rf"^ADM{level}_[A-Z][A-Z]$", column)
    ]
    official_names = [
        column
        for column in name_columns
        if match(r"^ADM._(AR|EN|ES|FR|RU|ZH)$", column)
    ]
    for column in official_names:
        lang = column.split("_")[1]
        gdf[f"ADM0_{lang}"] = get_adm0_name(iso3, lang.lower())
    if "ADM0_PCODE" not in gdf.columns:
        gdf["ADM0_PCODE"] = None
    gdf["ADM0_PCODE"] = gdf["ADM0_PCODE"].fillna(iso2)
    for column in name_columns:
        for char in apostrophe_chars:
            gdf[column] = gdf[column].str.replace(chr(int(char[2:], 16)), "'")
        for char in invisible_chars:
            gdf[column] = gdf[column].str.replace(chr(int(char[2:], 16)), "")
        gdf[column] = gdf[column].replace(r" +", " ", regex=True)
        gdf[column] = gdf[column].str.strip()
    return gdf.replace(r"^\s*$", None, regex=True)


def automatic_fixes(gdf: GeoDataFrame):
    gdf = gdf.to_crs(WGS84)
    gdf.geometry = gdf.geometry.force_2d()
    gdf.geometry = gdf.geometry.make_valid()
    for valid_to in ["ValidTo", "VALIDTO"]:
        if valid_to in gdf.columns:
            gdf = gdf.drop(columns=[valid_to])
    gdf["validTo"] = NaT
    gdf["validTo"] = gdf["validTo"].astype("date32[pyarrow]")
    return gdf


def config_fixes(gdf: GeoDataFrame, country_config: dict):
    if "date" in country_config:
        gdf["date"] = Timestamp(country_config["date"]).date()
    if "update" in country_config:
        gdf["validOn"] = Timestamp(country_config["update"]).date()
    if "drop" in country_config:
        gdf = gdf.drop(columns=country_config["drop"])
    if "duplicate" in country_config:
        for duplicate, original in country_config["duplicate"].items():
            gdf[duplicate] = gdf[original]
    if "rename" in country_config:
        gdf = gdf.rename(columns=country_config["rename"])
    if "title" in country_config:
        for column in country_config["title"]:
            gdf[column] = gdf[column].str.title()
    if "replace" in country_config:
        for column, replace in country_config["replace"].items():
            for key, value in replace.items():
                if key == "":
                    gdf[column] = gdf[column].fillna(value)
                else:
                    gdf[column] = gdf[column].str.replace(key, value)
    return gdf


def main() -> None:
    """Applies all issues in fix.json to files."""
    pbar = tqdm(countries)
    for country in pbar:
        iso3 = country.alpha_3
        iso2 = country.alpha_2
        pbar.set_postfix_str(iso3)
        admin_level = ADMIN_LEVEL_MAX
        country_config = level_1_fixes.get(iso3, {})
        sources = ["fix", "hdx", "itos"]
        if "level" in country_config:
            admin_level = country_config["level"]
            gdf = read_parquet(sources, iso3, admin_level)
        else:
            for level in range(ADMIN_LEVEL_MAX, -1, -1):
                admin_level = level
                gdf = read_parquet(sources, iso3, admin_level)
                if gdf is not None:
                    break
        if gdf is not None:
            gdf = config_fixes(gdf, country_config)
            gdf = automatic_fixes(gdf)
            gdf = name_fixes(gdf, iso3, iso2, admin_level)
            dissolve_and_save(gdf, iso3, admin_level)

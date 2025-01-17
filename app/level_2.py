from geopandas import GeoDataFrame, read_parquet
from pycountry import countries
from tqdm import tqdm

from .config import ADMIN_LEVEL_MAX, l1
from .utils import to_parquet


def refactor_columns(gdf: GeoDataFrame, admin_level: int):
    name_columns = [
        column
        for level in range(admin_level, -1, -1)
        for column in gdf.columns
        if match(rf"^ADM{level}_[A-Z][A-Z]$", column)
    ]
    for column in name_columns:
        gdf[column] = gdf[column].replace(r" +", " ", regex=True)
        gdf[column] = gdf[column].str.strip()
    return gdf


def main() -> None:
    """Applies all issues in fix.json to files."""
    pbar = tqdm(countries)
    for country in pbar:
        iso3 = country.alpha_3
        pbar.set_postfix_str(iso3)
        for admin_level in range(ADMIN_LEVEL_MAX, -1, -1):
            gdf = read_parquet(l1 / f"{iso3.lower()}_adm{admin_level}")
            if gdf is not None:
                to_parquet(gdf, iso3, admin_level, 2)

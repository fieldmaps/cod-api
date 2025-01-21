from pathlib import Path

from geopandas import read_parquet
from plotly.graph_objects import Choropleth, Figure
from tqdm import tqdm

from .config import outputs

EPSG_WGS84 = 4326
PLOTLY_SIMPLIFY = 0.000_1


def to_webp(file: Path) -> None:
    """Save file as images.

    Args:
        file: file to save.
    """
    gdf = read_parquet(file).to_crs(EPSG_WGS84)
    gdf = gdf[~gdf.geometry.is_empty]
    gdf.geometry = gdf.geometry.simplify(PLOTLY_SIMPLIFY)
    min_x, min_y, max_x, max_y = gdf.geometry.total_bounds
    fig = Figure(
        Choropleth(
            colorscale=["#1F77B4", "#1F77B4"],
            geojson=gdf.geometry.__geo_interface__,
            locations=gdf.index,
            marker_line_color="white",
            z=gdf.index,
        ),
    )
    fig.update_geos(
        bgcolor="rgba(0,0,0,0)",
        lataxis_range=[min_y, max_y],
        lonaxis_range=[min_x, max_x],
        visible=False,
    )
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )
    fig.update_traces(showscale=False)
    fig.write_image(file.with_suffix(".webp"), height=600, width=600)


def main() -> None:
    """Main function, runs all modules in sequence."""
    files = sorted(outputs.rglob("*.parquet"))
    pbar = tqdm(files)
    for file in pbar:
        pbar.set_postfix_str(file.stem)
        to_webp(file)


if __name__ == "__main__":
    main()

from pathlib import Path
from shutil import make_archive, rmtree
from subprocess import DEVNULL, run

from geopandas import read_file
from plotly.graph_objects import Choropleth, Figure
from tqdm import tqdm

from .config import boundaries, inputs

EPSG_WGS84 = 4326
PLOTLY_SIMPLIFY = 0.000_1


def tippecanoe(file: Path) -> None:
    """Save file as PMTiles.

    Args:
        file: file to save.
    """
    run(
        [
            "tippecanoe",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--maximum-zoom=g",
            "--simplify-only-low-zooms",
            "--force",
            f"--output={boundaries / file.stem}.pmtiles",
            boundaries / f"{file.stem}.geojson",
        ],
        check=False,
        stderr=DEVNULL,
    )


def plotly(file: Path) -> None:
    """Save file as images.

    Args:
        file: file to save.
    """
    gdf = read_file(file, use_arrow=True).to_crs(EPSG_WGS84)
    gdf = gdf[~gdf.geometry.is_empty]
    gdf.geometry = gdf.geometry.simplify(PLOTLY_SIMPLIFY)
    min_x, min_y, max_x, max_y = gdf.geometry.total_bounds
    fig = Figure(
        Choropleth(
            geojson=gdf.geometry.__geo_interface__,
            locations=gdf.index,
            z=gdf.index,
            colorscale=["#1F77B4", "#1F77B4"],
            marker_line_color="white",
        ),
    )
    fig.update_geos(
        bgcolor="rgba(0,0,0,0)",
        visible=False,
        lonaxis_range=[min_x, max_x],
        lataxis_range=[min_y, max_y],
    )
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )
    fig.update_traces(showscale=False)
    for suffix in ["png", "webp", "svg", "pdf"]:
        fig.write_image(boundaries / f"{file.stem}.{suffix}", height=2000, width=2000)


def main() -> None:
    """Main function, runs all modules in sequence."""
    files = sorted(inputs.glob("*.gpkg"))
    pbar = tqdm(files)
    for file in pbar:
        pbar.set_postfix_str(file.stem)
        for suffix in [
            "csv",
            "gdb",
            "geojson",
            "gpkg",
            "kml",
            "shp.zip",
            "xlsx",
        ]:
            output = boundaries / f"{file.stem}.{suffix}"
            options = ["-lco", "ENCODING=UTF-8"] if suffix.startswith("shp") else []
            run(
                ["ogr2ogr", "-overwrite", output, file, *options],
                stderr=DEVNULL,
                check=False,
            )
            if output.is_dir():
                make_archive(str(output), "zip", output)
                rmtree(output, ignore_errors=True)
        tippecanoe(file)
        plotly(file)


if __name__ == "__main__":
    main()

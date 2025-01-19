from pathlib import Path
from subprocess import DEVNULL, run

from tqdm import tqdm

from .config import outputs


def to_geojsonl(file: Path) -> None:
    """Save file as GeoJSONSeq.

    Args:
        file: GeoParquet file.
    """
    run(
        ["ogr2ogr", "-overwrite", file.with_suffix(".geojsonl"), file],
        check=False,
    )


def to_pmtiles(file: Path) -> None:
    """Save file as PMTiles.

    Args:
        file: GeoParquet file.
    """
    run(
        [
            "tippecanoe",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--force",
            "--maximum-zoom=g",
            "--read-parallel",
            "--simplify-only-low-zooms",
            f"--output={file.with_suffix('.pmtiles')}",
            file.with_suffix(".geojsonl"),
        ],
        check=False,
        stderr=DEVNULL,
    )


def main() -> None:
    """Main function, runs all modules in sequence."""
    files = sorted(outputs.rglob("*.parquet"))
    pbar = tqdm(files)
    for file in pbar:
        pbar.set_postfix_str(file.stem)
        to_geojsonl(file)
        to_pmtiles(file)
        file.with_suffix(".geojsonl").unlink()


if __name__ == "__main__":
    main()

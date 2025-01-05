from argparse import ArgumentParser
from pathlib import Path
from subprocess import run

cwd = Path(__file__).parent

parser = ArgumentParser()
parser.add_argument("--format", required=True, help="input format")
parser.add_argument("--input", required=True, help="input directory")
parser.add_argument("--output", required=True, help="output directory")
args = parser.parse_args()

if __name__ == "__main__":
    input_files = (cwd / args.input).rglob(f"*.{args.format}")
    for input_file in input_files:
        output_file = cwd / args.output / f"{input_file.stem}.parquet"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.unlink(missing_ok=True)
        run(
            [
                "ogr2ogr",
                *["-nlt", "PROMOTE_TO_MULTI"],
                *["-lco", "COMPRESSION=ZSTD"],
                *["-lco", "GEOMETRY_ENCODING=GEOARROW"],
                *["-lco", "GEOMETRY_NAME=geometry"],
                *["-mapFieldType", "DateTime=Date"],
                *[output_file, input_file],
            ],
            check=False,
        )

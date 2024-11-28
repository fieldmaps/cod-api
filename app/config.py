from pathlib import Path

cwd = Path(__file__).parent
inputs = cwd / "../inputs"
boundaries = cwd / "../outputs/boundaries"
boundaries.mkdir(parents=True, exist_ok=True)
stac = cwd / "../outputs/stac"
stac.mkdir(parents=True, exist_ok=True)

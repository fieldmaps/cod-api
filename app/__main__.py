from . import gdal, stac


def main() -> None:
    """Main function, runs all modules in sequence."""
    gdal.main()
    stac.main()


if __name__ == "__main__":
    main()

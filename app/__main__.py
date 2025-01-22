from . import images, level_0, level_1, pmtiles, stac


def main() -> None:
    """Main function, runs all modules in sequence."""
    level_0.main()
    level_1.main()
    pmtiles.main()
    images.main()
    stac.main()


if __name__ == "__main__":
    main()

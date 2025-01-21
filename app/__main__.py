from . import level_0, level_1, pmtiles, stac


def main() -> None:
    """Main function, runs all modules in sequence."""
    if False:
        level_0.main()
        level_1.main()
        pmtiles.main()
    stac.main()


if __name__ == "__main__":
    main()

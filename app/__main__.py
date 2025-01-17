from . import level_0, level_1


def main() -> None:
    """Main function, runs all modules in sequence."""
    level_0.main()
    level_1.main()


if __name__ == "__main__":
    main()

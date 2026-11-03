"""Headless entry points; help never connects to a broker or starts research."""

import argparse
import sys


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("mt5", "compatibility"))
    if not args:
        parser.print_help()
        return
    command = parser.parse_args(args[:1]).command
    if command == "mt5":
        from stratosphere.mt5.__main__ import main as run
    else:
        from stratosphere.phase0.compatibility import main as run
    run(args[1:])


if __name__ == "__main__":
    main()

import argparse
import pathlib
import sys

from yadd.duplicates import Processor
from yadd.util import iter_regular_files, format_size


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('root_dirs', nargs='*', type=pathlib.Path)
    parser.add_argument('-i', '--stdin', action='store_true')

    args = parser.parse_args()

    if bool(args.root_dirs) == args.stdin:
        parser.error('Exactly one of root_dirs and --stdin must be specified.')

    return args


def main(root_dirs, stdin):
    def iter_all_paths():
        if stdin:
            for line in sys.stdin:
                if line.endswith('\n'):
                    line = line[:-1]

                yield pathlib.Path(line)
        else:
            for root_dir in root_dirs:
                yield from iter_regular_files(root_dir)

    duplicates = Processor().find_duplicates(iter_all_paths())

    for paths, size, hash in sorted(sorted(i for i in duplicates)):
        print()
        print('{} files with {} and SHA 256 hash {}...:'.format(len(paths), format_size(size), hash[:16]))

        for path in paths:
            print(path)


def entry_point():
    main(**vars(parse_args()))

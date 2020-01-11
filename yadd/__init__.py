import argparse
import pathlib
import sys

from yadd.duplicates import find_duplicates
from yadd.statusline import StatusLine
from yadd.util import iter_regular_files, format_size


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('paths', nargs='*', type=pathlib.Path)
    parser.add_argument('-i', '--stdin', action='store_true')

    args = parser.parse_args()

    if bool(args.paths) == args.stdin:
        parser.error('Exactly one of root_dirs and --stdin must be specified.')

    return args


def main(paths, stdin):
    def iter_all_paths():
        if stdin:
            for line in sys.stdin:
                if line.endswith('\n'):
                    line = line[:-1]

                yield pathlib.Path(line)
        else:
            for path in paths:
                if path.is_dir():
                    yield from iter_regular_files(path)
                else:
                    yield path

    status_line = StatusLine.create()
    files_processed = 0
    data_read = 0
    duplicates_found = 0

    def update_status():
        status_line.set(
            '{} files, {} read, {} duplicates ...',
            files_processed,
            format_size(data_read),
            duplicates_found)

    def file_processed_progress_fn():
        nonlocal files_processed

        files_processed += 1
        update_status()

    def data_read_progress_fn(bytes):
        nonlocal data_read

        data_read += bytes
        update_status()

    def duplicate_found_progress_fn():
        nonlocal duplicates_found

        duplicates_found += 1
        update_status()

    duplicates = find_duplicates(
        iter_all_paths(),
        file_processed_progress_fn=file_processed_progress_fn,
        data_read_progress_fn=data_read_progress_fn,
        duplicate_found_progress_fn=duplicate_found_progress_fn,
        logger=status_line)

    status_line.clear()
    status_line.log('{} groups of identical files have been found.', len(duplicates))

    for paths, size, hash in sorted(sorted(i for i in duplicates)):
        print()
        print('{} files with {} and SHA 256 hash {}...:'.format(len(paths), format_size(size), hash[:16]))

        for path in paths:
            print(path)


def entry_point():
    main(**vars(parse_args()))

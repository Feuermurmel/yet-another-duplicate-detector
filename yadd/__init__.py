import argparse
import collections
import hashlib
import io
import itertools
import os
import pathlib
import sys
import time


class Progress:
    def __init__(self, file=sys.stderr):
        self._file = file
        self._last_time = 0
        self._last_progress = ''

        self._is_a_tty = self._file.isatty()

    def _update(self):
        if self._is_a_tty:
            print(
                self._last_progress + '\x1b[K\x1b[G',
                end='',
                file=self._file,
                flush=True)
        else:
            print(self._last_progress, file=self._file, flush=True)

    def set_progress(self, message, *args):
        current_time = time.time()

        if current_time > self._last_time + 0.2:
            self._last_time = current_time
            self._last_progress = message.format(*args)
            self._update()

    def clear(self):
        if self._is_a_tty:
            self._last_progress = ''
            self._update()

    def log(self, message, *args):
        if self._is_a_tty:
            # Not flushing so that _update() can flush the whole thing.
            print(message.format(*args) + '\x1b[K', file=self._file)

            self._update()
        else:
            print(message.format(*args), file=self._file, flush=True)


def iter_regular_files(root: pathlib.Path):
    for dirpath, dirnames, filenames in os.walk(str(root)):
        for i in filenames:
            path = root / dirpath / i

            if path.is_file() and not path.is_symlink():
                yield path


class HashFile(io.RawIOBase):
    def __init__(self, hash=hashlib.sha256):
        self.hash = hash()

    def write(self, b):
        self.hash.update(b)


def copy_file_part(fsrc, fdst, size):
    while size > 0:
        data = fsrc.read(min(size, 1 << 14))

        if not data:
            break

        size -= len(data)
        fdst.write(data)


block_size = 1 << 12


class File:
    def __init__(self, path: pathlib.Path, progress):
        self.path = path
        self._progress = progress

        self._indicators = []
        self._indicators_iter = self._iter_indicators()

    def _hash_part(self, pos, size):
        hash_file = HashFile()

        with self.path.open('rb') as file:
            file.seek(pos)
            copy_file_part(file, hash_file, size)

        return hash_file.hash.digest().hex()

    def _iter_indicators(self):
        size = self.path.stat().st_size

        yield 'size', size

        for i in itertools.count():
            pos = ((1 << i) - 1) * block_size

            if pos >= size:
                break

            yield 'block at {}'.format(pos), self._hash_part(pos, block_size)

        # Do not log small files.
        if size >= 1 << 24:
            self._progress.log('Fully hashing {} ({:.1f} MB) ...', self.path, size / 1e6)

        yield 'file hash', self._hash_part(0, size)

    def get_indicators_prefix(self, size):
        while size > len(self._indicators):
            element = next(self._indicators_iter, None)

            # Return a short list instead of raising an exception.
            if element is None:
                break

            self._indicators.append(element)

        return self._indicators[:size]


def find_duplicates(paths_iter, progress):
    # Keys prefixes of the indicators of a file as tuples. Values are either a
    # single File instance or ... if the entry has spilled because the
    # indicator prefix was not unique.
    files_by_indicator_prefixes = {}
    duplicate_paths_by_indicators = collections.defaultdict(list)

    def insert(file, depth):
        indicators = tuple(file.get_indicators_prefix(depth))

        if len(indicators) < depth:
            duplicate_paths_by_indicators[indicators].append(file.path)
        else:
            entry = files_by_indicator_prefixes.get(indicators)

            if entry is None:
                files_by_indicator_prefixes[indicators] = file
            else:
                insert(file, depth + 1)

                if entry is not ...:
                    files_by_indicator_prefixes[indicators] = ...
                    insert(entry, depth + 1)

    for path in paths_iter:
        insert(File(path, progress), 1)

    return list(duplicate_paths_by_indicators.values())


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('root_dirs', nargs='*', type=pathlib.Path)
    parser.add_argument('-i', '--stdin', action='store_true')

    args = parser.parse_args()

    if bool(args.root_dirs) == args.stdin:
        parser.error('Exactly one of root_dirs and --stdin must be specified.')

    return args


def main(root_dirs, stdin):
    progress = Progress()

    def iter_all_paths():
        if stdin:
            for line in sys.stdin:
                if line.endswith('\n'):
                    line = line[:-1]

                yield pathlib.Path(line)
        else:
            for root_dir in root_dirs:
                yield from iter_regular_files(root_dir)

    def iter_all_paths_with_progress():
        files_count = 0

        for i in iter_all_paths():
            files_count += 1
            progress.set_progress('Processing {} files ...', files_count)

            yield i

    duplicates = find_duplicates(iter_all_paths_with_progress(), progress)

    progress.clear()
    progress.log('{} groups of identical files have been found.', len(duplicates))

    for group in duplicates:
        print()

        for path in group:
            print(path)


def entry_point():
    main(**vars(parse_args()))

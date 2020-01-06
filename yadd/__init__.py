import argparse
import collections
import hashlib
import io
import itertools
import os
import pathlib
import sys
import time


class ProgressLine:
    def __init__(self, file):
        self._file = file

    def set(self, message, *args):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def log(self, message, *args):
        raise NotImplementedError

    @classmethod
    def create(cls, file=sys.stderr):
        if file.isatty():
            return TTYProgressLine(file)
        else:
            return NoTTYProgressLine(file)


class TTYProgressLine(ProgressLine):
    def __init__(self, *args):
        super().__init__(*args)

        self._last_time = 0
        self._last_progress = ''

    def _write_progress(self):
        print(
            self._last_progress + '\x1b[K\x1b[G',
            end='',
            file=self._file,
            flush=True)

    def set(self, message, *args):
        current_time = time.time()

        if current_time > self._last_time + 0.2:
            self._last_time = current_time
            self._last_progress = message.format(*args)
            self._write_progress()

    def clear(self):
        self._last_progress = ''
        self._write_progress()

    def log(self, message, *args):
        # Not flushing so that _write_progress() can flush the whole thing.
        print(message.format(*args) + '\x1b[K', file=self._file)

        self._write_progress()


class NoTTYProgressLine(ProgressLine):
    def set(self, message, *args):
        pass

    def clear(self):
        pass

    def log(self, message, *args):
        print(message.format(*args), file=self._file, flush=True)


def format_size(size):
    if size < 1000:
        return '{} bytes'.format(size)

    for unit in 'KMGTPEZY':
        size = size / 1000

        if size < 10:
            return '{:.2f} {}B'.format(size, unit)
        elif size < 100:
            return '{:.1f} {}B'.format(size, unit)
        elif size < 1000 or unit == 'Y':
            return '{:.0f} {}B'.format(size, unit)


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


def copy_file_part(fsrc, fdst, size, progress_fn=None):
    if progress_fn is None:
        progress_fn = lambda bytes: None

    while size > 0:
        data = fsrc.read(min(size, 1 << 14))

        if not data:
            break

        read_size = len(data)
        size -= read_size
        fdst.write(data)

        progress_fn(read_size)


block_size = 1 << 12


class File:
    def __init__(self, path: pathlib.Path, processor: 'Processor'):
        self.path = path
        self._processor = processor

        self._indicators = []
        self._indicators_iter = self._iter_indicators()

    def _hash_part(self, pos, size):
        def progress_fn(bytes):
            self._processor._data_read += bytes
            self._processor._update_progress()

        hash_file = HashFile()

        with self.path.open('rb') as file:
            file.seek(pos)
            copy_file_part(file, hash_file, size, progress_fn)

        return hash_file.hash.digest().hex()

    def _iter_indicators(self):
        size = self.path.stat().st_size

        yield 'size', size

        for i in itertools.count():
            pos = ((1 << i) - 1) * block_size
            read_size = min(block_size, size - pos)

            if read_size <= 0:
                break

            yield 'block at {}'.format(pos), self._hash_part(pos, read_size)

        # Do not log small files.
        if size >= 1 << 24:
            self._processor._progress_line.log('Fully hashing {} ({}) ...', self.path, format_size(size))

        yield 'file hash', self._hash_part(0, size)

    def get_indicators_prefix(self, size):
        while size > len(self._indicators):
            element = next(self._indicators_iter, None)

            # Return a short list instead of raising an exception.
            if element is None:
                break

            self._indicators.append(element)

        return self._indicators[:size]


class Processor:
    def __init__(self):
        self._files_processed = 0
        self._data_read = 0
        self._duplicates_found = 0

        self._progress_line = ProgressLine.create()

    def _update_progress(self):
        self._progress_line.set(
            '{} files, {} read, {} duplicates ...',
            self._files_processed,
            format_size(self._data_read),
            self._duplicates_found)

    def find_duplicates(self, paths_iter):
        self._update_progress()

        # Keys prefixes of the indicators of a file as tuples. Values are
        # either a single File instance or ... if the entry has spilled because
        # the indicator prefix was not unique.
        files_by_indicator_prefixes = {}
        duplicate_paths_by_indicators = collections.defaultdict(list)

        def insert(file, depth):
            indicators = tuple(file.get_indicators_prefix(depth))

            if len(indicators) < depth:
                duplicate_paths_by_indicators[indicators].append(file.path)

                self._duplicates_found += 1
                self._update_progress()
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
            insert(File(path, self), 1)

            self._files_processed += 1
            self._update_progress()

        def iter_duplicates():
            for indicator, paths in duplicate_paths_by_indicators.items():
                # Extract size and full hash of file.
                yield sorted(paths), indicator[0][1], indicator[-1][1]

        duplicates = sorted(iter_duplicates())

        self._progress_line.clear()
        self._progress_line.log('{} groups of identical files have been found.', len(duplicates))

        return duplicates


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

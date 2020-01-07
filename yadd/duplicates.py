import collections
import functools
import itertools
import pathlib
import typing

from yadd.util import hash_file_part, Logger, format_size


class _File:
    """
    Wraps the path of a regular file and provides access to prefixes of a
    lazily calculated list of indicators, which can be used to tell two files
    apart.
    """

    def __init__(
            self,
            path: pathlib.Path,
            indicators_iter: typing.Iterator[typing.Any]):
        self.path = path

        self._indicators_iter = indicators_iter
        self._indicators = []

    def get_indicators_prefix(self, length: int):
        """
        Return a prefix of the list of indicators of this file.

        Longer prefixes are more work to calculate but also are more likely to
        tell two files apart that are indeed different.
        """

        while length > len(self._indicators):
            element = next(self._indicators_iter, None)

            # Return a short list instead of raising an exception.
            if element is None:
                break

            self._indicators.append(element)

        return self._indicators[:length]


_block_size = 1 << 12


def find_duplicates(
        paths_iter: typing.Iterable[pathlib.Path],
        *, file_processed_progress_fn: typing.Callable[[], None],
        data_read_progress_fn: typing.Callable[[int], None],
        duplicate_found_progress_fn: typing.Callable[[], None],
        logger: Logger):
    # Keys prefixes of the indicators of a file as tuples. Values are either a
    # single File instance or `...`, if the entry has spilled because the
    # indicator prefix was not unique.
    files_by_indicator_prefixes = {}
    duplicate_paths_by_indicators = collections.defaultdict(list)

    def insert(file, depth):
        indicators = tuple(file.get_indicators_prefix(depth))

        if len(indicators) < depth:
            duplicate_paths_by_indicators[indicators].append(file.path)
            duplicate_found_progress_fn()
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
        def iter_indicators(path=path):
            hash_part = functools.partial(
                hash_file_part,
                path,
                progress_fn=data_read_progress_fn)

            size = path.stat().st_size

            yield 'size', size

            for i in itertools.count():
                pos = ((1 << i) - 1) * _block_size
                read_size = min(_block_size, size - pos)

                if read_size <= 0:
                    break

                yield 'block at {}'.format(pos), hash_part(pos, read_size)

            # Do not log small files.
            if size >= 1 << 24:
                logger.log('Fully hashing {} ({}) ...', path, format_size(size))

            yield 'file hash', hash_part(0, size)

        insert(_File(path, iter_indicators()), 1)
        file_processed_progress_fn()

    def iter_duplicates():
        for indicators, paths in duplicate_paths_by_indicators.items():
            # Extract size and full hash of file.
            yield sorted(paths), indicators[0][1], indicators[-1][1]

    duplicates = sorted(iter_duplicates())

    return duplicates

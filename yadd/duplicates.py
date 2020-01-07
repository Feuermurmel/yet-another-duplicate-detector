import collections

from yadd.util import format_size
from yadd.file import File
from yadd.statusline import StatusLine


class Processor:
    def __init__(self):
        self._files_processed = 0
        self._data_read = 0
        self._duplicates_found = 0

        self._status_line = StatusLine.create()

    def _update_status(self):
        self._status_line.set(
            '{} files, {} read, {} duplicates ...',
            self._files_processed,
            format_size(self._data_read),
            self._duplicates_found)

    def find_duplicates(self, paths_iter):
        self._update_status()

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
                self._update_status()
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
            self._update_status()

        def iter_duplicates():
            for indicator, paths in duplicate_paths_by_indicators.items():
                # Extract size and full hash of file.
                yield sorted(paths), indicator[0][1], indicator[-1][1]

        duplicates = sorted(iter_duplicates())

        self._status_line.clear()
        self._status_line.log('{} groups of identical files have been found.', len(duplicates))

        return duplicates

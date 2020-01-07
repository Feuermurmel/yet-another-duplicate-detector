import hashlib
import io
import itertools
import pathlib

from yadd.util import format_size, copy_file_part


class _HashFile(io.RawIOBase):
    """
    A simple file-like object which calculates a hash of all data written to
    it.
    """

    def __init__(self, hash=hashlib.sha256):
        self.hash = hash()

    def write(self, b):
        self.hash.update(b)


_block_size = 1 << 12


class File:
    """
    Wraps the path of a regular file and provides access to prefixes of a
    lazily calculated list of indicators, which can be used to tell two files
    apart.
    """

    def __init__(self, path: pathlib.Path, processor):
        self.path = path
        self._processor = processor

        self._indicators = []
        self._indicators_iter = self._iter_indicators()

    def _hash_part(self, pos, size):
        def progress_fn(bytes):
            self._processor._data_read += bytes
            self._processor._update_status()

        hash_file = _HashFile()

        with self.path.open('rb') as file:
            file.seek(pos)
            copy_file_part(file, hash_file, size, progress_fn)

        return hash_file.hash.digest().hex()

    def _iter_indicators(self):
        size = self.path.stat().st_size

        yield 'size', size

        for i in itertools.count():
            pos = ((1 << i) - 1) * _block_size
            read_size = min(_block_size, size - pos)

            if read_size <= 0:
                break

            yield 'block at {}'.format(pos), self._hash_part(pos, read_size)

        # Do not log small files.
        if size >= 1 << 24:
            self._processor._progress_line.log('Fully hashing {} ({}) ...', self.path, format_size(size))

        yield 'file hash', self._hash_part(0, size)

    def get_indicators_prefix(self, size):
        """
        Return a prefix of the list of indicators of this file.

        Longer prefixes are more work to calculate but also are more likely to
        tell two files apart that are indeed different.
        """

        while size > len(self._indicators):
            element = next(self._indicators_iter, None)

            # Return a short list instead of raising an exception.
            if element is None:
                break

            self._indicators.append(element)

        return self._indicators[:size]

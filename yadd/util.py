import os
import pathlib
import typing


def format_size(size: int):
    """
    Format a file size in bytes into a human-readable string, e.g. "15 bytes"
    or 12.3 TB. Uses decimal SI-prefixes.
    """

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


def copy_file_part(
        fsrc: typing.BinaryIO,
        fdst: typing.BinaryIO,
        size: int,
        progress_fn: typing.Callable[[int], None] = None):
    """
    Variant of shutil.copyfile() which allows only a section of a file to be
    copied and allows a callback to be specified over which copy progress is
    reported.

    If the source file does not have enough bytes left, the copy operation
    stops early without raising an error.

    :param fsrc: The file-like object to read the data from.
    :param fdst: The file-like object to write the data to.
    :param size: The number of bytes to copy.
    :param progress_fn: A function which is called with the number of bytes
    copied for each block copied.
    """

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


def iter_regular_files(root: pathlib.Path):
    """
    Return an iterator yielding paths to all regular files under the specified
    directory.
    """

    for dirpath, dirnames, filenames in os.walk(str(root)):
        for i in filenames:
            path = pathlib.Path(dirpath) / i

            if path.is_file() and not path.is_symlink():
                yield path

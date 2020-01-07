import abc
import sys
import time
import typing

from yadd.util import Logger


class StatusLine(Logger):
    """
    Wraps a file-like object and abstract displaying a progress/status-line
    on the last line of the console which still allowing log lines to be
    printed to the same console.

    The status line is not updated more often than once every 0.2 second.

    When output is not written to a console, the status lines and any ANSI
    escape codes are omitted.
    """

    def __init__(self, file):
        self._file = file

    def set(self, message: str, *args):
        """
        Set the new content of the status line.

        str.format() is automatically called on the message with the specified
        arguments.
        """

        raise NotImplementedError

    def clear(self):
        """
        Delete output printed in the status line from the console.
        """

        raise NotImplementedError

    def log(self, message, *args):
        """
        Write a "normal" line of output to the console, not affecting the
        display pf the status line.

        str.format() is automatically called on the message with the specified
        arguments.
        """

        raise NotImplementedError

    @classmethod
    def create(cls, file: typing.BinaryIO = sys.stderr):
        if file.isatty():
            return _TTYStatusLine(file)
        else:
            return _NoTTYStatusLine(file)


class _TTYStatusLine(StatusLine):
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


class _NoTTYStatusLine(StatusLine):
    def set(self, message, *args):
        pass

    def clear(self):
        pass

    def log(self, message, *args):
        print(message.format(*args), file=self._file, flush=True)

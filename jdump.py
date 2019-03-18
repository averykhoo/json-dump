import glob
import gzip
import io
import json
import os
import warnings
from typing import Union


def _reader(file_obj, separator):
    """
    don't call this from outside the class pls
    not very useful to you since it doesn't parse the json

    :param file_obj: file-like object in 'rt' mode
    :param separator: expected separator
    """
    json_buffer = []
    for line in file_obj:

        # append until we reach json object separator
        if line.rstrip('\r\n') != separator:
            json_buffer.append(line)
            continue

        # reached separator, yield complete json object buffer
        yield json_buffer
        json_buffer = []

    # make sure no data was dropped
    assert not ''.join(json_buffer).strip(), f'input json must end with {repr(separator)}'


class DumpReader:
    def __init__(self, f, separator='--', unique=True):
        """
        :param f: file-like object (expects mode='rt')
        :param separator:
        :param unique: skip (do not yield) duplicate objects
        """
        self._reader = _reader(f, separator)
        self.obj_num = 0
        self.file_obj = f
        if unique:
            self.seen = set()
        else:
            self.seen = None

    def __iter__(self):
        return self

    def __next__(self):
        json_obj = json.loads(''.join(next(self._reader)))

        # if UNIQUE flag is set
        if self.seen is not None:
            json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
            while json_hash in self.seen:
                json_obj = json.loads(''.join(next(self._reader)))
                json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
            self.seen.add(json_hash)

        self.obj_num += 1
        return json_obj

    def read(self, n=-1):
        """
        read up to n items from the file
        if less than n items remain in the file, return those items
        if n < 0, reads all items

        :param n: how many items to read
        :return: list of n items
        """
        ret = []
        if n >= 0:
            for _ in range(n):
                try:
                    ret.append(next(self))
                except StopIteration:
                    break
        else:
            for obj in self:
                ret.append(obj)
        return ret

    def skip(self, n=1):
        """
        skip up to n items from the file
        does not count towards unique objects returned

        :param n: how many items to skip
        :return: num objects skipped
        """
        assert n > 0

        num_skipped = 0

        try:
            for _ in range(n):
                next(self._reader)
                num_skipped += 1
        except StopIteration:
            pass

        return num_skipped


class DumpWriter:
    def __init__(self, f, separator='--', unique=True, indent=4):
        """
        :param f: file-like object (expects mode='wt')
        :param separator:
        :param unique: skip (do not write) duplicate objects
        :param indent: see python's json docs
        """
        self.separator_blob = f'\n{separator}\n'
        self.file_obj = f
        self.obj_num = 0
        self.indent = indent
        if unique:
            self.seen = set()
        else:
            self.seen = None

    def write(self, json_obj):
        """
        write a single json object
        (given a list, writes the entire list as a single object)

        :param json_obj:
        :return:
        """
        formatted_json = json.dumps(json_obj, indent=self.indent, sort_keys=True, ensure_ascii=False, allow_nan=False)

        # if UNIQUE flag is set
        if self.seen is not None:
            json_hash = hash(formatted_json)
            if json_hash in self.seen:
                return False
            self.seen.add(json_hash)

        self.file_obj.write(formatted_json + self.separator_blob)
        self.obj_num += 1
        return True

    def writemany(self, json_iterator):
        """
        write multiple json objects from an iterator
        (given a list, writes each item in the list separately)

        :param json_iterator:
        :return:
        """
        return sum(self.write(obj) for obj in json_iterator)


class DumpOpener:
    def __init__(self, path, mode='r', gz=None, unique=True):
        """

        :param path:
        :param mode:
        :param gz:
        :param unique:
        """
        # verify mode
        if mode not in 'rwax':
            raise IOError(f'Mode "{mode}" not supported')
        self.mode = mode

        # normalize path
        self.path = os.path.abspath(path)
        self.temp_path = None

        # init file objects
        self.file_obj = None
        self.gz = None
        self.rw_obj = None

        # read/append mode (don't create new file)
        if mode in 'ra':

            # determine whether to use gzip
            if gz is None:
                with io.open(self.path, mode='rb') as f:
                    b = f.read(2)
                    if b == b'\x1f\x8b':
                        _open = gzip.open
                    else:
                        _open = io.open

            elif gz:
                _open = gzip.open

            else:
                _open = io.open

            # create file obj and reader/writer
            if mode == 'r':
                self.file_obj = _open(self.path, mode='rt', encoding='utf8')
                self.rw_obj = DumpReader(self.file_obj, unique=unique)
            else:
                self.file_obj = _open(self.path, mode='at', encoding='utf8')
                self.rw_obj = DumpWriter(self.file_obj, unique=unique)

        # write/create mode (create new file)
        else:
            if mode == 'x' and os.path.exists(self.path):
                raise FileExistsError(f'File already exists: {self.path}')

            # normalize filename
            filename = os.path.basename(self.path)
            self.temp_path = self.path + '.partial'

            # correct the filename for a compressed file
            if filename.lower().endswith('.gz'):
                filename = filename[:-3]

            # detect gzipped file
            if filename.lower().endswith('gz'):
                self.gz = True

            # determine whether to use gzip
            if gz is None:
                if self.gz:
                    # _open = gzip.open
                    self.file_obj = io.open(self.temp_path, mode='wb')
                    self.gz = io.TextIOWrapper(gzip.GzipFile(filename=filename, mode='wb', fileobj=self.file_obj),
                                               encoding='utf8')
                else:
                    self.file_obj = io.open(self.temp_path, mode='wt', encoding='utf8')
            elif gz:
                # _open = gzip.open
                self.file_obj = io.open(self.temp_path, mode='wb')
                self.gz = io.TextIOWrapper(gzip.GzipFile(filename=filename, mode='wb', fileobj=self.file_obj),
                                           encoding='utf8')
            else:
                # _open = open
                self.file_obj = io.open(self.temp_path, mode='wt', encoding='utf8')

            # # open file and return writer
            if self.gz is None:
                self.rw_obj = DumpWriter(self.file_obj, unique=unique)
            else:
                self.rw_obj = DumpWriter(self.gz, unique=unique)

    def __enter__(self) -> Union[DumpReader, DumpWriter]:
        return self.rw_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.gz is not None:
            self.gz.close()

        self.file_obj.close()

        if self.temp_path is not None:
            if os.path.exists(self.path):
                if self.mode == 'x':
                    raise FileExistsError(f'File was created during writing: {self.path}')
                os.remove(self.path)
            os.rename(self.temp_path, self.path)


def load(input_glob, unique=True, verbose=True):
    """
    yields json objects from multiple files matching some glob pattern
    auto-detects gzip compression for each file

    :param input_glob: pattern to match
    :param unique: yield only unique items
    :param verbose: print filenames being loaded
    """
    input_paths = sorted(glob.glob(os.path.abspath(input_glob), recursive=True))
    if not input_paths:
        warnings.warn(f'zero files found matching <{input_glob}>')

    # re-implement unique to remove duplicates from multiple files
    if unique:
        seen = set()
    else:
        seen = None

    for i, path in enumerate(input_paths):
        if verbose:
            print(f'[{i + 1}/{len(input_paths)}] ({os.path.getsize(path)}) {path}')

        with DumpOpener(path) as f:
            for json_obj in f:
                if seen is not None:
                    json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
                    if json_hash in seen:
                        continue
                    seen.add(json_hash)
                yield json_obj


def dump(json_iterator, path, overwrite=True, unique=True):
    """
    like json.dump but writes many objects to a single output file

    :param json_iterator: iterator over json objects to be written
    :param path: output path
    :param overwrite: overwrite existing file, if any
    :param unique: don't write duplicates
    :return: number of objects written
    """
    with DumpOpener(path, mode='w' if overwrite else 'x', unique=unique) as f:
        return f.writemany(json_iterator)


# be more like the gzip library
open = DumpOpener

# be more like the csv library
reader = DumpReader
writer = DumpWriter

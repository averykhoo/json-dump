import glob
import gzip
import io
import json
import os
import warnings
from pathlib import Path, PurePath
from typing import Union


def format_bytes(num):
    """
    string formatting
    :type num: int
    :rtype: str
    """
    num = abs(num)
    if num == 0:
        return '0 Bytes'
    elif num == 1:
        return '1 Byte'
    unit = 0
    while num >= 1024 and unit < 8:
        num /= 1024.0
        unit += 1
    unit = ['Bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'][unit]
    return ('%.2f %s' if num % 1 else '%d %s') % (num, unit)


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
    def __init__(self, f, unique=True, separator='--'):
        """
        :param f: file-like object in read-text mode
        :param unique: skip (do not yield) duplicate objects
        :param separator:
        """
        self._reader = _reader(f, separator)
        self.count = 0
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

        self.count += 1
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
    def __init__(self, f, unique=True, separator='--', indent=4):
        """
        :param f: file-like object in write-text mode
        :param unique: skip (do not write) duplicate objects
        :param separator:
        :param indent: see python's json docs
        """
        self.separator_blob = f'\n{separator}\n'  # '\r\n' conversion handled by textIO's newline
        self.file_obj = f
        self.count = 0
        self.indent = indent
        if unique:
            self.seen = set()
        else:
            self.seen = None

    def write(self, json_obj):
        """
        write a single json object
        (given a list, writes the entire list as a single object)

        :param json_obj: object to write
        :return: True if object was written, else False (eg. for duplicates)
        """
        formatted_json = json.dumps(json_obj, indent=self.indent, sort_keys=True, ensure_ascii=False, allow_nan=False)

        # if UNIQUE flag is set
        if self.seen is not None:
            json_hash = hash(formatted_json)
            if json_hash in self.seen:
                return False
            self.seen.add(json_hash)

        self.file_obj.write(formatted_json + self.separator_blob)
        self.count += 1
        return True

    def writemany(self, json_iterator):
        """
        write multiple json objects from an iterator
        (given a list, writes each item in the list separately)

        :param json_iterator:
        :return: number of objects written
        """
        return sum(self.write(obj) for obj in json_iterator)


class DumpFile:
    rw_obj: Union[DumpReader, DumpWriter]

    def __init__(self, path, mode='r', write_gz=False, unique=True, encoding='utf8', newline='\n'):
        """
        note that existing items are not accounted for uniqueness when appending

        :param path: string / pathlib.Path / pathlib.PurePath
        :param mode: (r)ead, (w)rite, (a)ppend, e(x)clusive creation
        :param write_gz: use gzip compression; if a string, sets the filename for writing
        :param unique: only read/write unique objects
        :param encoding: strongly recommended that you stick with utf-8
        :param newline: recommended that you stick with '\n'
        """
        # verify mode is legit
        if mode not in 'rwax':
            raise IOError(f'Mode not supported: {repr(mode)}')

        # normalize path
        self.path = Path(path).resolve()

        # init file objects
        self.file_obj = None
        self.gz = None

        # read/append mode (don't create new file)
        if mode in 'ra':

            # determine whether to use gzip
            with io.open(str(self.path), mode='rb') as f:
                b = f.read(2)
                if b == b'\x1f\x8b':
                    _open = gzip.open
                else:
                    _open = io.open

            # create file obj and reader/writer
            if mode == 'r':
                self.file_obj = _open(str(self.path), mode='rt', encoding=encoding)
                self.rw_obj = DumpReader(self.file_obj, unique=unique)
            else:
                self.file_obj = _open(str(self.path), mode='at', encoding=encoding, newline=newline)
                self.rw_obj = DumpWriter(self.file_obj, unique=unique)

        # write/create mode (create new file)
        else:
            # if overwrite is disabled
            if mode == 'x' and self.path.exists():
                raise FileExistsError(f'File already exists: {self.path}')

            # prepare dir to write to
            if not self.path.parent.is_dir():
                assert not self.path.parent.exists(), 'parent dir is not dir'
                self.path.parent.mkdir(parents=True, exist_ok=True)

            # the GZIP flag is set
            if write_gz:
                # get the filename from flag if possible
                if isinstance(write_gz, str) or isinstance(write_gz, PurePath):
                    filename = os.path.basename(write_gz)  # maybe someone put in a full path

                # otherwise get from self.path
                else:
                    filename = self.path.name
                    if filename.lower().endswith('.partial'):
                        filename = filename[:-8]
                    if filename.lower().endswith('.gz'):
                        filename = filename[:-3]

                # _open = gzip.open
                self.file_obj = io.open(str(self.path), mode=mode + 'b')
                self.gz = io.TextIOWrapper(gzip.GzipFile(filename=filename, mode=mode + 'b', fileobj=self.file_obj),
                                           encoding=encoding, newline=newline)

            # don't use gzip
            else:
                if self.path.name.endswith('gz'):
                    warnings.warn(f'write_gz=False, but file path ends with "gz": {self.path}')
                self.file_obj = io.open(str(self.path), mode=mode + 't', encoding=encoding, newline=newline)

            # create writer
            if self.gz is None:
                self.rw_obj = DumpWriter(self.file_obj, unique=unique)
            else:
                self.rw_obj = DumpWriter(self.gz, unique=unique)

    def close(self):
        # first close the gzip textIO object
        if self.gz is not None:
            self.gz.close()
            self.gz = None

        # then close the actual file
        if self.file_obj is not None:
            self.file_obj.close()
            self.file_obj = None
        else:
            warnings.warn(f'File already closed: ({self.path})')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def read(self, n=-1):
        return self.rw_obj.read(n)

    def skip(self, n=1):
        return self.rw_obj.skip(n)

    def write(self, json_obj):
        return self.rw_obj.write(json_obj)

    def writemany(self, json_iterator):
        return self.rw_obj.writemany(json_iterator)

    def __iter__(self):
        return iter(self.rw_obj)

    def get_count(self):
        return self.rw_obj.count


def load(input_glob, unique=True, verbose=True):
    """
    yields json objects from multiple files matching some glob pattern
    auto-detects gzip compression for each file

    :param input_glob: recursive pattern to match
    :param unique: yield only unique items
    :param verbose: print filenames being loaded
    """
    # find files to read
    input_paths = sorted(filter(os.path.isfile, glob.glob(os.path.abspath(input_glob), recursive=True)))
    if not input_paths:
        warnings.warn(f'zero files found matching <{input_glob}>')

    # re-implement unique to remove duplicates from multiple files
    if unique:
        seen = set()
    else:
        seen = None

    # read all files
    for i, path in enumerate(input_paths):
        if verbose:
            print(f'[{i + 1}/{len(input_paths)}] ({format_bytes(os.path.getsize(path))}) {path}')

        with DumpFile(path, mode='r', unique=False) as f:
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
    writes to a temp file before finally renaming the file at the end

    :param json_iterator: iterator over json objects to be written
    :param path: output path
    :param overwrite: overwrite existing file, if any
    :param unique: don't write duplicates
    :return: number of objects written
    """

    # if not overwrite then skip
    if not overwrite and os.path.exists(path):
        return 0

    # set filename
    filename = os.path.basename(path)
    if filename.lower().endswith('.gz'):
        filename = filename[:-3]
    elif filename.lower().endswith('gz'):
        warnings.warn(f'GZIP is enabled but internal filename is: {filename}')
    else:
        filename = False

    # use a temp file
    temp_path = os.path.abspath(path) + '.partial'
    with DumpFile(temp_path, mode='w', write_gz=filename, unique=unique) as f:
        n_written = f.writemany(json_iterator)

    # remove original file
    if os.path.exists(path):
        if not overwrite:
            # someone else created the file we want to exclusively create while we were doing stuff
            raise FileExistsError(f'File was created during writing: {path}')
        os.remove(path)
    os.rename(temp_path, path)

    return n_written


# be more like the gzip library
# noinspection PyShadowingBuiltins
open = DumpFile

# be more like the csv library
reader = DumpReader
writer = DumpWriter

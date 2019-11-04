import glob
import gzip
import io
import json
import os
import warnings
from io import TextIOWrapper
from pathlib import Path
from typing import BinaryIO
from typing import Optional
from typing import TextIO
from typing import Union


def format_bytes(num_bytes):
    """
    :type num_bytes: int
    :rtype: str
    """

    # handle negatives
    if num_bytes < 0:
        minus = '-'
    else:
        minus = ''
    num_bytes = abs(num_bytes)

    # ±1 byte (singular form)
    if num_bytes == 1:
        return f'{minus}1 Byte'

    # determine unit
    unit = 0
    while unit < 8 and num_bytes > 999:
        num_bytes /= 1024.0
        unit += 1
    unit = ['Bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'][unit]

    # exact or float
    if num_bytes % 1:
        return f'{minus}{num_bytes:,.2f} {unit}'
    else:
        return f'{minus}{num_bytes:,.0f} {unit}'


def resolve_glob(glob_patterns):
    if isinstance(glob_patterns, (str, os.PathLike)):
        glob_patterns = [glob_patterns]

    paths = set()
    for glob_pattern in glob_patterns:
        assert isinstance(glob_pattern, (str, os.PathLike)), glob_pattern
        paths.update(glob.glob(os.path.abspath(glob_pattern), recursive=True))
    return sorted(paths)


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

    # dump files must end with separator, so we should never trigger this
    if ''.join(json_buffer).strip():
        warnings.warn(f'dump file did not end with {repr(separator)} and may be corrupt')
        yield json_buffer


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
            json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=True))
            while json_hash in self.seen:
                json_obj = json.loads(''.join(next(self._reader)))
                json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=True))
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

        :param n: how many items to skip (skip all if n<0)
        :return: num objects skipped
        """
        num_skipped = 0

        # skip a specific number of objects
        if n >= 0:
            try:
                for _ in range(n):
                    next(self._reader)
                    num_skipped += 1
            except StopIteration:
                pass

        # skip all remaining objects
        else:
            for _ in self._reader:
                num_skipped += 1

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
        formatted_json = json.dumps(json_obj, indent=self.indent, sort_keys=True, ensure_ascii=False, allow_nan=True)

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
    path: Path
    temp_path: Optional[Path]

    file_obj: Union[TextIO, BinaryIO, None]
    gz: Optional[TextIOWrapper]
    temp_lock: Optional[BinaryIO]

    rw_obj: Union[DumpReader, DumpWriter]

    def __init__(self, path, mode='r', encoding='utf8', write_gz=False, unique=True, newline='\n', write_temp=False):
        """
        note that existing items are not accounted for uniqueness when appending

        :param path: string / pathlib.Path / pathlib.PurePath
        :param mode: (r)ead, (w)rite, (a)ppend, e(x)clusive creation
        :param encoding: strongly recommended that you stick with utf-8
        :param write_gz: use gzip compression; if a string, sets the filename for writing
        :param unique: only read/write unique objects
        :param newline: recommended that you stick with '\n'
        :param write_temp: write to a temp (.partial) file
        """
        # verify mode is legit
        mode = mode.lower()
        if mode not in {'r', 'w', 'a', 'x'}:
            raise IOError(f'Mode not supported: {repr(mode)}')
        self.mode = mode

        # normalize path
        self.path = Path(path).resolve()
        assert not self.path.is_dir(), f'Target path is a directory: {self.path}'
        self.temp_path = None

        # init file objects
        self.file_obj = None
        self.gz = None
        # self.temp_lock = None  # write-lock target path if using a temp path

        # read/append mode (don't create new file)
        if self.mode in {'r', 'a'}:

            # warn that WRITE_TEMP flag is ignored
            if write_temp:
                warnings.warn(f'the WRITE_TEMP flag is ignored in {repr(self.mode)} self.mode')

            # warn that WRITE_GZIP flag is ignored
            if write_gz:
                warnings.warn(f'the WRITE_GZIP flag is ignored in {repr(self.mode)} self.mode')

            # determine whether to use gzip
            with io.open(str(self.path), mode='rb') as f:
                b = f.read(2)
                if b == b'\x1f\x8b':
                    _open = gzip.open
                else:
                    _open = io.open

            # create file obj and reader/writer
            if self.mode == 'r':
                self.file_obj = _open(str(self.path), mode='rt', encoding=encoding)
                self.rw_obj = DumpReader(self.file_obj, unique=unique)
            else:
                self.file_obj = _open(str(self.path), mode='at', encoding=encoding, newline=newline)
                self.rw_obj = DumpWriter(self.file_obj, unique=unique)

        # write/create self.mode (create new file)
        else:
            # if overwrite is disabled
            if self.mode == 'x' and self.path.exists():
                raise FileExistsError(f'File already exists: {self.path}')

            # prepare dir to write to
            if not self.path.parent.is_dir():
                assert not self.path.parent.exists(), f'Target parent directory is not a directory: {self.path.parent}'
                self.path.parent.mkdir(parents=True, exist_ok=True)

            # check the temp path
            if write_temp:
                self.temp_path = self.path.with_suffix(self.path.suffix + '.partial')
                if self.temp_path.exists():
                    assert not self.temp_path.is_dir()
                    self.temp_path.unlink()

            # the WRITE_GZIP flag is set
            if write_gz:
                # get the filename from flag if possible
                if isinstance(write_gz, (str, os.PathLike)):
                    filename = os.path.basename(write_gz)  # maybe someone put in a full path

                # otherwise get from self.path
                else:
                    filename = self.path.name
                    if filename.lower().endswith('.partial'):
                        filename = filename[:-8]
                    if filename.lower().endswith('.gz'):
                        filename = filename[:-3]

                # open file to write bytes
                if self.temp_path is not None:
                    # # chope original path
                    # if self.mode == 'x':
                    #     self.temp_lock = io.open(str(self.path), mode='xb')
                    # else:
                    #     self.temp_lock = io.open(str(self.path), mode='ab')  # don't overwrite
                    # temp path
                    self.file_obj = io.open(str(self.temp_path), mode=self.mode + 'b')
                else:
                    self.file_obj = io.open(str(self.path), mode=mode + 'b')

                # noinspection PyTypeChecker
                self.gz = io.TextIOWrapper(gzip.GzipFile(filename=filename, mode=mode + 'b', fileobj=self.file_obj),
                                           encoding=encoding, newline=newline)

            # don't use gzip
            else:
                # save my future self from forgetting the WRITE_GZIP flag
                if self.path.name.endswith('gz'):
                    warnings.warn(f'write_gz=False, but file path ends with "gz": {self.path}')

                # open text mode file
                if self.temp_path is not None:
                    # # chope original path
                    # if self.mode == 'x':
                    #     self.temp_lock = io.open(str(self.path), mode='xb')
                    # else:
                    #     self.temp_lock = io.open(str(self.path), mode='ab')  # don't overwrite
                    # temp path
                    self.file_obj = io.open(str(self.temp_path), mode=mode + 't', encoding=encoding, newline=newline)
                else:
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

        # # close choped file path
        # if self.temp_lock is not None:
        #     self.temp_lock.close()
        #     self.temp_lock = None

        # rename from temp path
        if self.temp_path is not None:
            if self.temp_path.is_file():
                if self.path.exists():
                    self.path.unlink()
                self.temp_path.rename(self.path)
                self.temp_path = None
            else:
                warnings.warn(f'Temp file does not exist: ({self.temp_path})')

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

    def flush(self):
        if self.gz is not None:
            self.gz.flush()
        self.file_obj.flush()


def load(glob_paths, unique=True, verbose=True):
    """
    yields json objects from multiple files matching some glob pattern
    auto-detects gzip compression for each file

    :param glob_paths: recursive pattern to match
    :param unique: yield only unique items
    :param verbose: print filenames being loaded
    """

    # find files to read
    input_paths = [path for path in resolve_glob(glob_paths) if os.path.isfile(path)]

    # no files to read
    if not input_paths:
        warnings.warn(f'zero files found matching {glob_paths}')

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
                    json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=True))
                    if json_hash in seen:
                        continue
                    seen.add(json_hash)
                yield json_obj


def dump(json_iterator, paths, overwrite=True, unique=True):
    """
    like json.dump but writes many objects to a single output file
    writes to a temp file before finally renaming the file at the end

    :param json_iterator: iterator over json objects to be written
    :param paths: output path, or list of paths
    :param overwrite: overwrite any existing files (otherwise do nothing)
    :param unique: don't write duplicates
    :return: number of objects written
    """
    # normalize `paths` to list of strings
    if isinstance(paths, (str, os.PathLike)):
        paths = [os.path.abspath(paths)]
    paths = [os.path.abspath(path) for path in paths]

    # no output paths
    if not len(paths):
        warnings.warn('zero output paths specified')
        return 0

    # if not overwrite then skip
    if not overwrite and any(os.path.exists(path) for path in paths):
        return 0

    # set filename
    filenames = []
    for path in paths:
        filename = os.path.basename(path)
        if filename.lower().endswith('.gz'):
            filename = filename[:-3]
        elif filename.lower().endswith('gz'):
            warnings.warn(f'GZIP is enabled but internal filename will match external filename: {filename}')
        else:
            filename = False
        filenames.append(filename)

    #  if OVERWRITE is unset then we want exclusive creation mode
    if overwrite:
        mode = 'w'
    else:
        mode = 'x'

    # make output files
    files = []
    for path, filename in zip(paths, filenames):
        f = DumpFile(path, mode=mode, write_gz=filename, unique=unique, write_temp=True)
        files.append(f)

    # write items
    if len(files) == 1:
        files[0].writemany(json_iterator)
    else:
        for json_obj in json_iterator:
            for f in files:
                f.write(json_obj)

    # close files
    n_written = None
    for f in files:
        if n_written is None:
            n_written = f.get_count()
        else:
            assert n_written == f.get_count()
        f.close()

    # remove original file
    return n_written


def get_count(glob_paths):
    """
    count number of items in a dump file

    :param glob_paths: files to read
    :return: number of items as a non-negative integer
    """

    # find files to read
    input_paths = [path for path in resolve_glob(glob_paths) if os.path.isfile(path)]

    # no files to read
    if not input_paths:
        warnings.warn(f'zero files found matching {glob_paths}')
        return 0

    # sum count over all files
    count = 0
    for path in input_paths:
        with DumpFile(path, mode='r', unique=False) as f:
            count += f.skip(-1)

    return count


# be more like the gzip library
# noinspection PyShadowingBuiltins
open = DumpFile

# be more like the csv library
reader = DumpReader
writer = DumpWriter

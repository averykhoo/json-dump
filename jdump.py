import glob
import gzip
import io
import json
import os
import warnings


def _reader(file_obj, separator='--'):
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
    assert not ''.join(json_buffer).strip(), 'input json must end with {repr(separator)} separator!'


class RogerReader:
    def __init__(self, f, separator='--', unique=True, close=False):
        self._reader = _reader(f, separator)
        self.obj_num = 0
        self.file_obj = f
        self.close = close
        if unique:
            self.seen = set()
        else:
            self.seen = None

    def __iter__(self):
        return self

    def __next__(self):
        json_buffer = next(self._reader)
        json_obj = json.loads(''.join(json_buffer))

        # if UNIQUE flag is set
        if self.seen is not None:
            json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
            while json_hash in self.seen:
                json_buffer = next(self._reader)
                json_obj = json.loads(''.join(json_buffer))
                json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
            self.seen.add(json_hash)

        self.obj_num += 1
        return json_obj

    def read_n(self, n=1):
        ret = []
        for _ in range(n):
            try:
                ret.append(next(self))
            except StopIteration:
                break
        return ret

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('exiting')
        if self.close:
            self.file_obj.close()


class RogerWriter:
    def __init__(self, f, separator='--', unique=True, indent=4):
        self.separator_blob = f'\n{separator}\n'
        self.file_obj = f
        self.obj_num = 0
        self.close = False
        self.indent = indent
        if unique:
            self.seen = set()
        else:
            self.seen = None

    def write(self, json_obj):
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.close:
            self.file_obj.close()
        pass


class RogerOpen:
    def __init__(self, path, mode='r', gz=None, encoding='utf8', unique=True):
        # verify mode
        if mode not in 'rwax':
            raise IOError('Mode "{mode}" not supported')
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
                with open(self.path, mode='rb') as f:
                    b = f.read(2)
                    if b == b'\x1f\x8b':
                        _open = gzip.open
                    else:
                        _open = open

            elif gz:
                _open = gzip.open

            else:
                _open = open

            # create file obj and reader/writer
            if mode == 'r':
                self.file_obj = _open(self.path, mode='rt', encoding=encoding)
                self.rw_obj = RogerReader(self.file_obj, unique=unique)
            else:
                assert mode == 'a'
                self.file_obj = _open(self.path, mode='at', encoding=encoding)
                self.rw_obj = RogerWriter(self.file_obj, unique=unique)

        # write/create mode (create new file)
        else:
            assert mode in 'wx'
            if mode == 'x' and os.path.exists(self.path):
                raise FileExistsError(f'File exists: {self.path}')

                # normalize filename
            filename = os.path.basename(self.path)
            self.temp_path = self.path + '.partial'

            # handle compressed txt
            if filename.endswith('.gz'):
                filename = filename[:-3]
                self.gz = True

            # some other gzip file
            if filename.endswith('gz'):
                self.gz = True

            # determine whether to use gzip
            if gz is None:
                if self.gz:
                    # _open = gzip.open
                    self.file_obj = open(self.temp_path, mode='wb')
                    self.gz = io.TextIOWrapper(gzip.GzipFile(filename=filename, mode=mode + 'b', fileobj=self.file_obj),
                                               encoding=encoding)
                else:
                    self.file_obj = open(self.temp_path, mode='wt', encoding=encoding)
            elif gz:
                # _open = gzip.open
                self.file_obj = open(self.temp_path, mode='wb')
                self.gz = io.TextIOWrapper(gzip.GzipFile(filename=filename, mode=mode + 'b', fileobj=self.file_obj),
                                           encoding=encoding)
            else:
                # _open = open
                self.file_obj = open(self.temp_path, mode='wt', encoding=encoding)

            # # open file and return writer
            if self.gz is None:
                self.rw_obj = RogerWriter(self.file_obj, unique=unique)
            else:
                self.rw_obj = RogerWriter(self.gz, unique=unique)

    def __enter__(self):
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


def yield_json(input_glob, unique=True, verbose=True):
    input_paths = sorted(glob.glob(os.path.abspath(input_glob), recursive=True))
    if not input_paths:
        warnings.warn(f'zero files found matching <{input_glob}>')

    if unique:
        seen = set()
    else:
        seen = None

    for i, path in enumerate(input_paths):
        if verbose:
            print(f'[{i+1}/{len(input_paths)}] ({os.path.getsize(path)}) {path}')

        with RogerOpen(path) as f:
            for json_obj in f:
                if seen is not None:
                    json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
                    if json_hash in seen:
                        continue
                    seen.add(json_hash)
                yield json_obj


def write_json(json_iterator, path, overwrite=False, unique=True, write_blank=True):
    with RogerOpen(path, mode='w' if overwrite else 'x', unique=True) as f:
        for json_obj in json_iterator:
            f.write(json_obj)


if __name__ == '__main__':
    with RogerOpen('test.txt', 'w') as f:
        f.write({'test': 1})

    with RogerOpen('test.txt') as f:
        print(1)
        print(f.read_n(10))

    with RogerOpen('test.txt', 'a') as f:
        f.write({'test': 2})

    print(2)
    for j in yield_json('test.txt*'):
        print(j)

    # os.remove('test.txt.gz')
    with RogerOpen('test.txt.gz', 'w') as f:
        f.write({'test': 3})

    # os.remove('test.txt.gz')
    with RogerOpen('test.txt.gz') as f:
        print(3)
        print(f.read_n(10))

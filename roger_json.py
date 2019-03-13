import gzip
import json


def _reader(file_obj, separator='--'):
    separator = separator + '\n'

    json_buffer = []
    for line in file_obj:

        # append until we reach json object separator
        if line != separator:
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
    def __init__(self, f, separator='--', unique=True, close=False):
        self.separator_blob = f'\n{separator}\n'
        self.file_obj = f
        self.obj_num = 0
        self.close = False
        if unique:
            self.seen = set()
        else:
            self.seen = None

    def write(self, json_obj):
        formatted_json = json.dumps(json_obj, indent=4, sort_keys=True, ensure_ascii=False, allow_nan=False)

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
    def __init__(self, path, mode='r', gz=None):
        if mode not in 'rwax':
            raise IOError('Mode "{mode}" not supported')

        if mode == 'r':
            # determine whether to use gzip
            if gz is None:
                with open(path, mode='rb') as f:
                    b = f.read(2)
                    if b == b'\x1f\x8b':
                        _open = gzip.open
                    else:
                        print('text_mode', repr(b))
                        _open = open
            elif gz:
                _open = gzip.open
            else:
                _open = open

            # open file and return reader
            self.file_obj = _open(path, mode='rt', encoding='utf8')
            self.rw_obj = RogerReader(self.file_obj)

        else:
            # determine whether to use gzip
            if gz is None:
                if path.endswith('gz'):
                    _open = gzip.open
                else:
                    _open = open
            elif gz:
                _open = gzip.open
            else:
                _open = open

            # open file and return writer
            self.file_obj = _open(path, mode=mode + 't', encoding='utf8')
            self.rw_obj = RogerWriter(self.file_obj)

    def __enter__(self):
        return self.rw_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_obj.close()
        pass





if __name__ == '__main__':
    with RogerOpen('test.txt.gz', 'w') as f:
        f.write({'test': 1})
    # print(1)
    # f.file_obj.close()
    # print(2)

    with RogerOpen('test.txt.gz') as f:
        print(f.read_n(10))

    with RogerOpen('test.txt.gz', 'a') as f:
        f.write({'test': 2})

    with RogerOpen('test.txt.gz') as f:
        for j in f:
            print(j)

    with RogerOpen('test.txt.gz', 'x') as f:
        f.write({'test': 2})

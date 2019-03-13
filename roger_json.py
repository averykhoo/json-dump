import gzip
import io
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
        # yield json.loads(''.join(json_buffer))
        json_buffer = []

    # make sure no data was dropped
    assert not ''.join(json_buffer).strip(), 'input json must end with "--" separator!'


class RogerReader(object):
    def __init__(self, f, separator='--', unique=True):
        self._reader = _reader(f, separator)
        self.obj_num = 0
        if unique:
            self.seen = set()
        else:
            self.seen = None

    def __iter__(self):
        return self

    def next(self):
        json_buffer = self._reader.next()
        json_obj = json.loads(''.join(json_buffer))

        # if UNIQUE flag is set
        if self.seen is not None:
            json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
            while json_hash in self.seen:
                json_buffer = self._reader.next()
                json_obj = json.loads(''.join(json_buffer))
                json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
            self.seen.add(json_hash)

        self.obj_num += 1
        return json_obj

    def read_n(self, n=1):
        ret = []
        for _ in range(n):
            try:
                ret.append(self.next())
            except StopIteration:
                break
        return ret


class RogerWriter(object):
    def __init__(self, f, separator='--', unique=True):
        self.separator_blob = '\n' + separator + '\n'
        self.file_obj = f
        self.obj_num = 0
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


def ropen(path, mode='r', gz=None):
    if mode not in 'rwax':
        raise IOError, 'Mode ' + mode + ' not supported'

    if mode == 'r':
        # determine whether to use gzip
        if gz is None:
            with open(path, mode='rb') as f:
                if f.read(2) == b'\x1f\x8b':
                    _open = gzip.open
                else:
                    _open = io.open
        elif gz:
            _open = gzip.open
        else:
            _open = io.open

        # open file and return reader
        with _open(path, mode='rt', encoding='utf8') as f:
            return RogerReader(f)

    else:
        # determine whether to use gzip
        if gz is None:
            if path.endswith('gz'):
                _open = gzip.open
            else:
                _open = io.open
        elif gz:
            _open = gzip.open
        else:
            _open = io.open

        # open file and return writer
        with _open(path, mode=mode + 't', encoding='utf8') as f:
            return RogerWriter(f)


if __name__ == '__main__':
    with ropen('test.txt.gz', 'w') as f:
        f.write({'test':1})


    with ropen('test.txt.gz') as f:
        print(f.read_many(10))

    with ropen('test.txt.gz', 'a') as f:
        f.write({'test':2})

    with ropen('test.txt.gz') as f:
        for j in f:
            print(j)

    with ropen('test.txt.gz', 'x') as f:
        f.write({'test':2})
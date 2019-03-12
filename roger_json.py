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

def _write(file_obj, json_iterator, separator='--', max_io_attempts=3):
    separator = separator + '\n'

    for json_obj in json_iterator:
        json_str
        for io_attempt in range(max_io_attempts):
            try:
                file_obj.write()

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
        json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
        if self.seen is not None:
            while json_hash in self.seen:
                json_buffer = self._reader.next()
                json_obj = json.loads(''.join(json_buffer))
                json_hash = hash(json.dumps(json_obj, sort_keys=True, ensure_ascii=False, allow_nan=False))
            self.seen.add(json_hash)
        self.obj_num += 1
        return json_obj


class RogerWriter(object):
    def __init__(self, f, fieldnames, restval="", extrasaction="raise",
                 dialect="excel", *args, **kwds):
        self.fieldnames = fieldnames  # list of keys for the dict
        self.restval = restval  # for writing short dicts
        if extrasaction.lower() not in ("raise", "ignore"):
            raise ValueError, \
                ("extrasaction (%s) must be 'raise' or 'ignore'" %
                 extrasaction)
        self.extrasaction = extrasaction
        self.writer = writer(f, dialect, *args, **kwds)

    def writeheader(self):
        header = dict(zip(self.fieldnames, self.fieldnames))
        self.writerow(header)

    def _dict_to_list(self, rowdict):
        if self.extrasaction == "raise":
            wrong_fields = [k for k in rowdict if k not in self.fieldnames]
            if wrong_fields:
                raise ValueError("dict contains fields not in fieldnames: "
                                 + ", ".join([repr(x) for x in wrong_fields]))
        return [rowdict.get(key, self.restval) for key in self.fieldnames]

    def writerow(self, rowdict):
        return self.writer.writerow(self._dict_to_list(rowdict))

    def writerows(self, rowdicts):
        rows = []
        for rowdict in rowdicts:
            rows.append(self._dict_to_list(rowdict))
        return self.writer.writerows(rows)

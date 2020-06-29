#   JSON dump
Dump multiple JSON objects into a single file

-   NOTE: Default behavior is to drop duplicates when reading/writing, set `unique=False` to read/write all objects
-   NOTE: Default behavior is to sort keys, since this was written prior to Python 3.7


##  Usage

### Read everything from a single filepath (or from multiple files matching a single glob pattern)
```python
from pprint import pprint
import jdump

# mimics `json.load` but is an iterator yielding json objects
for json_obj in jdump.load('some/glob/path/**/filename.*'):  # also accepts Path objects
    pprint(json_obj)
```

### Read everything from multiple filepaths or glob patterns
```python
from pathlib import Path
from pprint import pprint
import jdump

# mimics `json.load` but is an iterator yielding json objects
for json_obj in jdump.load(['some/glob/path/**/filename.*', 
                            'some/other/path/filename2.txt', 
                            Path('yet/another/path/filename3.txt.gz')]):
    pprint(json_obj)
```

### Write objects to a single filepath
```python
import jdump

json_objs = [{'example': n} for n in range(100)]

# write to a plaintext file
jdump.dump(json_objs, 'path/to/file.txt')  # also accepts Path objects

# to write a gzip-compressed text file, just append ".gz" to the path
jdump.dump(json_objs, 'path/to/file.txt.gz')
```

### Write objects to multiple filepaths
```python
from pathlib import Path
import jdump

json_objs = [{'example': n} for n in range(100)]

jdump.dump(json_objs, ['path/to/file.txt', 
                       'path/to/another_file.txt.gz',
                       Path('yet/another/file.txt.gz')])
```


## Advanced Usage

### Open a file (read/write/append/create)
-   Usage of `jdump.open` is similar to `io.open` or `gzip.open`, but you can feed it any json-like object
-   Valid modes are `r`, `w`, `a`, and `x`
-   To compress, set `write_gz` to your preferred filename (or to `True` if you want to be lazy)
-   Gzip compression is auto-detected when reading/appending
-   To write to a temp file, set `write_temp` to true

```python
from pprint import pprint
import jdump

# read an existing file (gzip is auto-detected)
with jdump.open('path/to/file.txt', mode='r') as f:
    for json_obj in f:
        pprint(json_obj)

# write to a file (create or overwrite)
with jdump.open('path/to/file.txt', mode='w') as f:
    f.write({'example': ['object']})

# write to a gzipped file
with jdump.open('path/to/file.txt.gz', mode='w', write_gz='file.txt') as f:
    f.write({'example': ['object']})

# append to an existing file (gzip is auto-detected)
with jdump.open('path/to/file.txt', mode='a') as f:
    f.write({'example': ['object']})

# write to a new file (exclusive creation)
with jdump.open('path/to/file.txt.gz', mode='x', write_gz='file.txt') as f:
    f.write({'example': ['object']})
```


### Write multiple objects to a file using `writemany`
```python
import jdump

json_objs = [{'example': n} for n in range(100)]

# RECOMMENDED
jdump.dump(json_objs, 'path/to/file.txt')

# using open and `writemany`
with jdump.open('path/to/file.txt.gz', mode='w', write_gz='file.txt') as f:
    n_written = f.writemany(json_objs)  # returns number of objects written
print(f'wrote {n_written} objects')

# equivalent to the following snippet using `write`
with jdump.open('path/to/file.txt.gz', mode='w', write_gz='file.txt') as f:
    n_written = 0
    for json_obj in json_objs:
        n_written += f.write(json_obj)  # returns True if object is written
```


### Reading/writing on already-opened file objects
-   Usage of `jdump.reader` and `jdump.writer` is similar to `csv.reader` and `csv.writer`
```python
import gzip
from pprint import pprint
import jdump

# read a plaintext file
with gzip.open('some_file.txt.gz', 'rt') as f: 
    d = jdump.reader(f)
    for json_obj in d:
        pprint(json_obj)

# write a gzipped file
with open('some_file.txt', 'wt', encoding='utf8') as f: 
    d = jdump.writer(f)
    d.write({'example': ['object']})
```


### Editing objects
-   you should read and write to separate files to avoid loading everything into memory at once
-   this also spreads out the IO load, which is (on my spinning rust) usually higher than the CPU load
```python
import jdump

with jdump.open('output.txt.gz', mode='w', write_gz=True) as f:
    for json_obj in jdump.load('input.txt'):
        json_obj['content'] = 'hello world'
        f.write(json_obj)  # note that `json_obj` is a dictionary, not a string
```


### Other `DumpFile` methods/attributes
-   `DumpFile.path` <-- path of the file (as a `pathlib.Path` object)
-   `DumpFile.get_count()` <-- how many items have been read/written since the file was opened
-   `DumpFile.skip(n)` <-- skip reading an object (which will be excluded from the read count)
-   `DumpFile.flush()` <-- does what you'd expect it to do


### Other `jdump` methods
-   `jdump.get_count` <-- counts number of (non-unique) objects in a file (or multiple paths/pathlib.Paths/globs)


## Misc

###  Why `--` as a separator?
-   As far as I can tell, this is very unlikely to exist in valid JSON

###  Why force `\n` as the newline ending?
-   Because *some* people assume unix-style line endings and hard code these things

###  Why does `jdump.dump` use temp files?
-   So that dumps are transactional: either all objects are dumped, or the path isn't (over)written

###  Why is the UNIQUE flag on by default?
-   I personally don't need duplicate objects returned when reading my files

### Why not [json](https://docs.python.org/3/library/json.html)
-   From the docs:
    **Note:** Unlike pickle and marshal, JSON is not a framed protocol, 
              so trying to serialize multiple objects with repeated calls to dump() 
              using the same fp will result in an invalid JSON file.

### Why not [Avro](https://fastavro.readthedocs.io/en/latest/) or [Parquet](https://arrow.apache.org/docs/python/parquet.html)
-   Why not indeed

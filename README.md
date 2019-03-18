#   JSON dump

The original page for this project is `https://github.com/averykhoo/json_dump`

NOTE: Default behavior is to drop duplicates when reading/writing, set `unique=False` to read/write all objects



##  Usage

### Open a file to read/write/append/create
-   Usage of `jdump.open` is similar to `io.open` or `gzip.open`
-   Valid modes are `r`, `w`, `a`, and `x`
```python
import jdump
from pprint import pprint

# read an existing (possibly gzipped) file
with jdump.open('path/to/file.txt', mode='r') as f:
    for json_obj in f:
        pprint(json_obj)
        
# append to an existing file
with jdump.open('path/to/file.txt', mode='a') as f:
    f.write({'example': ['object']})

# write to a file (create or overwrite)
with jdump.open('path/to/file.txt', mode='w') as f:
    f.write({'example': ['object']})

# write to a file (overwrite only)
with jdump.open('path/to/file.txt', mode='w') as f:
    f.write({'example': ['object']})
```


### Read everything from multiple files matching some glob pattern
```python
import jdump
from pprint import pprint

# mimics `json.load` but is an iterator yielding json objects
for json_obj in jdump.load('some/glob/path/**/filename.*'):
    pprint(json_obj)
```


### To write a *gzipped* dumpfile
-   Just append `.gz` to the filename
-   Gzip compression is auto-detected when reading/appending
```python
import jdump

with jdump.open('path/to/file.txt.gz', mode='w') as f:
    f.write({'example': ['object']})
```


### Write multiple objects to a file
```python
import jdump

json_objs = [{'example': n} for n in range(100)]

# using open and `write`
with jdump.open('path/to/file.txt.gz', mode='w') as f:
    for json_obj in json_objs:
        f.write(json_obj)  # returns True
        
# using open and `writemany`
with jdump.open('path/to/file.txt.gz', mode='w') as f:
    f.writemany(json_objs)  # returns number of objects written
    
# use `jdump.dump` which takes an iterator and a path (unlike `json.dump` which takes a file object)
jdump.dump(json_objs, 'path/to/file.txt')
```


### Reading/writing on already opened file objects
-   Usage of `jdump.reader` and `jdump.writer` is similar to `csv.reader` and `csv.writer`
```python
import jdump
from pprint import pprint
import gzip

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



##  Why `--`
-   As far as I can tell, this is very unlikely to exist in valid JSON

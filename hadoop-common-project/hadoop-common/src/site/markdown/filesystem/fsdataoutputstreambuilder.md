<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!--  ============================================================= -->
<!--  CLASS: FSDataOutputStreamBuilder -->
<!--  ============================================================= -->

# class `org.apache.hadoop.fs.FSDataOutputStreamBuilder`

<!-- MACRO{toc|fromDepth=1|toDepth=2} -->

Builder pattern for `FSDataOutputStream` and its subclasses. It is used to
create a new file or open an existing file on `FileSystem` for write.

## Invariants

The `FSDataOutputStreamBuilder` interface does not validate parameters
and modify the state of `FileSystem` until `build()` is
invoked.

## Implementation-agnostic parameters.

### <a name="Builder.create"></a> `FSDataOutputStreamBuilder create()`

Specify `FSDataOutputStreamBuilder` to create a file on `FileSystem`, equivalent
to `CreateFlag#CREATE`.

### <a name="Builder.append"></a> `FSDataOutputStreamBuilder append()`

Specify `FSDataOutputStreamBuilder` to append to an existing file on
`FileSystem`, equivalent to `CreateFlag#APPEND`.

### <a name="Builder.overwrite"></a> `FSDataOutputStreamBuilder overwrite(boolean overwrite)`

Specify `FSDataOutputStreamBuilder` to overwrite an existing file or not. If
giving `overwrite==true`, it truncates an existing file, equivalent to
`CreateFlag#OVERWITE`.

### <a name="Builder.permission"></a> `FSDataOutputStreamBuilder permission(FsPermission permission)`

Set permission for the file.

### <a name="Builder.bufferSize"></a> `FSDataOutputStreamBuilder bufferSize(int bufSize)`

Set the size of the buffer to be used.

### <a name="Builder.replication"></a> `FSDataOutputStreamBuilder replication(short replica)`

Set the replication factor.

### <a name="Builder.blockSize"></a> `FSDataOutputStreamBuilder blockSize(long size)`

Set block size in bytes.

### <a name="Builder.recursive"></a> `FSDataOutputStreamBuilder recursive()`

Create parent directories if they do not exist.

### <a name="Builder.progress"></a> `FSDataOutputStreamBuilder progress(Progresable prog)`

Set the facility of reporting progress.

### <a name="Builder.checksumOpt"></a> `FSDataOutputStreamBuilder checksumOpt(ChecksumOpt chksumOpt)`

Set checksum opt.

### Set optional or mandatory parameters

    FSDataOutputStreamBuilder opt(String key, ...)
    FSDataOutputStreamBuilder must(String key, ...)

Set optional or mandatory parameters to the builder. Using `opt()` or `must()`,
client can specify FS-specific parameters without inspecting the concrete type
of `FileSystem`.

    // Don't
    if (fs instanceof FooFileSystem) {
        FooFileSystem fs = (FooFileSystem) fs;
        out = dfs.createFile(path)
            .optionA()
            .optionB("value")
            .cache()
            .build()
    } else if (fs instanceof BarFileSystem) {
        ...
    }

    // Do
    out = fs.createFile(path)
        .permission(perm)
        .bufferSize(bufSize)
        .opt("foofs:option.a", true)
        .opt("foofs:option.b", "value")
        .opt("barfs:cache", true)
        .must("foofs:cache", true)
        .must("barfs:cache-size", 256 * 1024 * 1024)
        .build();

#### Implementation Notes

The concrete `FileSystem` and/or `FSDataOutputStreamBuilder` implementation
MUST verify that implementation-agnostic parameters (i.e., "syncable`) or
implementation-specific parameters (i.e., "foofs:cache")
are supported. `FileSystem` will satisfy optional parameters (via `opt(key, ...)`)
on best effort. If the mandatory parameters (via `must(key, ...)`) can not be satisfied
in the `FileSystem`, `IllegalArgumentException` must be thrown in `build()`.

The behavior of resolving the conflicts between the parameters set by
builder methods (i.e., `bufferSize()`) and `opt()`/`must()` is as follows:

> The last option specified defines the value and its optional/mandatory state.

## HDFS-specific parameters.

`HdfsDataOutputStreamBuilder extends FSDataOutputStreamBuilder` provides additional
HDFS-specific parameters, for further customize file creation / append behavior.

### `FSDataOutpuStreamBuilder favoredNodes(InetSocketAddress[] nodes)`

Set favored DataNodes for new blocks.

### `FSDataOutputStreamBuilder syncBlock()`

Force closed blocks to the disk device. See `CreateFlag#SYNC_BLOCK`

### `FSDataOutputStreamBuilder lazyPersist()`

Create the block on transient storage if possible.

### `FSDataOutputStreamBuilder newBlock()`

Append data to a new block instead of the end of the last partial block.

### `FSDataOutputStreamBuilder noLocalWrite()`

Advise that a block replica NOT be written to the local DataNode.

### `FSDataOutputStreamBuilder ecPolicyName()`

Enforce the file to be a striped file with erasure coding policy 'policyName',
no matter what its parent directory's replication or erasure coding policy is.

### `FSDataOutputStreamBuilder replicate()`

Enforce the file to be a replicated file, no matter what its parent directory's
replication or erasure coding policy is.

## Builder interface

### <a name="Builder.build"></a> `FSDataOutputStream build()`

Create a new file or append an existing file on the underlying `FileSystem`,
and return `FSDataOutputStream` for write.

#### Preconditions

The following combinations of parameters are not supported:

    if APPEND|OVERWRITE: raise HadoopIllegalArgumentException
    if CREATE|APPEND|OVERWRITE: raise HadoopIllegalArgumentExdeption

`FileSystem` may reject the request for other reasons and throw `IOException`,
see `FileSystem#create(path, ...)` and `FileSystem#append()`.

#### Postconditions

    FS' where :
       FS'.Files'[p] == []
       ancestors(p) is-subset-of FS'.Directories'

    result = FSDataOutputStream

The result is `FSDataOutputStream` to be used to write data to filesystem.


## <a name="s3a"></a> S3A-specific options

Here are the custom options which the S3A Connector supports.

| Name                        | Type      | Meaning                                |
|-----------------------------|-----------|----------------------------------------|
| `fs.s3a.create.performance` | `boolean` | create a file with maximum performance |
| `fs.s3a.create.header`      | `string`  | prefix for user supplied headers       |
| `fs.s3a.create.multipart`   | `boolean` | create a multipart file                |

### `fs.s3a.create.performance`

Prioritize file creation performance over safety checks for filesystem consistency.

This:
1. Skips the `LIST` call which makes sure a file is being created over a directory.
   Risk: a file is created over a directory.
2. If the overwrite flag is false and filesystem flag`fs.s3a.create.conditional.enabled` is true, 
   uses conditional creation to prevent the overwrite of any object at the destination.
3. Never issues a `DELETE` call to delete parent directory markers.

It is possible to probe an S3A Filesystem instance for this capability through
the `hasPathCapability(path, "fs.s3a.create.performance")` check.

Creating files with this option over existing directories is likely
to make S3A filesystem clients behave inconsistently.

Operations optimized for directories (e.g. listing calls) are likely
to see the directory tree not the file; operations optimized for
files (`getFileStatus()`, `isFile()`) more likely to see the file.
The exact form of the inconsistencies, and which operations/parameters
trigger this are undefined and may change between even minor releases.

Using this option is the equivalent of pressing and holding down the
"Electronic Stability Control"
button on a rear-wheel drive car for five seconds: the safety checks are off.
Things wil be faster if the driver knew what they were doing.
If they didn't, the fact they had held the button down will
be used as evidence at the inquest as proof that they made a
conscious decision to choose speed over safety and
that the outcome was their own fault.

Note: the option can be set for an entire filesystem. Again, the safety checks
are there to more closely match the semantics of a classic filesystem,
and to reduce the likelihood that the object store ends up in a state which
diverges so much from the classic directory + tree structur that applications
get confused.

Accordingly: *Use if and only if you are confident that the conditions are met.*

### `fs.s3a.create.header` User-supplied header support

Options with the prefix `fs.s3a.create.header.` will be added to the
S3 object metadata as "user defined metadata".
This metadata is visible to all applications. It can also be retrieved through the
FileSystem/FileContext `listXAttrs()` and `getXAttrs()` API calls with the prefix `header.`

When an object is renamed, the metadata is propagated the copy created.

It is possible to probe an S3A Filesystem instance for this capability through
the `hasPathCapability(path, "fs.s3a.create.header")` check.

### `fs.s3a.create.multipart` Create a multipart file

Initiate a multipart upload when a file is created, rather
than only when the amount of data buffered reaches the threshold
set in `fs.s3a.multipart.size`.

This is only relevant during testing, as it allows for multipart
operation to be initiated without writing any data, so
reducing test time.

It is not recommended for production use, because as well as adding
more network IO, it is not compatible with third-party stores which
do not supprt multipart uploads.

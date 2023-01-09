<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Doris Thirdparty Libs

This repository is used to manage third-party libraries used in Apache Doris. Some libraries have not been maintained for a long time, so fork into this repository for bug fixes and feature development. Each third-party library is in a separate branch.

# Current Libs

| Lib Name | Branch   | Description                                                  | Base version | Source URL                                                   | Latest Tag | CHANGELOG|
| -------- | -------- | ------------------------------------------------------------ | ------------ | ------------------------------------------------------------ | ---------- | --- |
| libhdfs3 | [libhdfs3](https://github.com/apache/doris-thirdparty/tree/libhdfs3) | designed as an alternative implementation of libhdfs, is implemented based on native Hadoop RPC protocol and HDFS data transfer protocol. It gets rid of the drawbacks of JNI, and it has a lightweight, small memory footprint code base. In addition, it is easy to use and deploy. | Master       | [HAWQ_depends](https://github.com/apache/hawq/tree/master/depends/libhdfs3) | libhdfs3-v2.3.4     | [CHANGELOG](https://github.com/apache/doris-thirdparty/blob/libhdfs3/CHANGELOG.md) |
| bdbje    | [bdbje](https://github.com/apache/doris-thirdparty/tree/bdbje)    | Berkley Database Java Edition - build and runtime support.   | 18.3.12      | [bdbje Maven src](https://search.maven.org/artifact/com.sleepycat/je/18.3.12/jar) | bdbje-18.3.14-doris-snapshot    | [CHANGELOG](https://github.com/apache/doris-thirdparty/blob/bdbje/CHANGELOG.md) |
| datatables | [datatables](https://github.com/apache/doris-thirdparty/tree/datatables)  |    | 1.12.1      | | 1.12.1    | [CHANGELOG](https://github.com/apache/doris-thirdparty/blob/datatables/CHANGELOG.md) |
| clucene | [clucene](https://github.com/apache/doris-thirdparty/tree/clucene)  | CLucene is a C++ port of Lucene: the high-performance, full-featured text search engine written in Java. CLucene is faster than lucene as it is written in C++.   | Master      | [clucene](https://sourceforge.net/p/clucene/code/ci/master/tree/)| libclucene-v2.4.1    | [CHANGELOG](https://github.com/apache/doris-thirdparty/blob/clucene/ChangeLog) |

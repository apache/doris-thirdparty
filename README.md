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

# Oracle NoSQL Database Server

Berkley Database Java Edition - build and runtime support.

| Licenses     | [Apache 2](http://www.apache.org/licenses/LICENSE-2.0.txt)   |
| :----------- | ------------------------------------------------------------ |
| Home page    | http://www.oracle.com/technetwork/database/database-technologies/nosqldb |
| Source code  | http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html |
| Organization | Oracle Corporation                                           |
| Developers   | Oracle Corporation                                           |

# Alternatives

A fork of com.sleepycat:je:18.3.12 from [bdbje](https://repo1.maven.org/maven2/com/sleepycat/je/18.3.12/je-18.3.12-sources.jar), Applied patches from [StarRocks bdbje](https://github.com/StarRocks/bdb-je/).
Because of  StarRocks bdbje based on version 7.x, Apache Doris use bdbje  base on version 18.x.  So cannot use StarRocks bdb-je directly.

# Deploy maven snapshot

1. Preparation

    Following the instruction of [Maven Release Preparation](https://doris.apache.org/community/release-and-verify/release-prepare/#maven-release-preparation)

2. Change the version in pom.xml

    Change the version in `pom.xml` and create a tag.

    Push the tag to the repo.

3. Deploy

    Run: `mvn deploy`

    If you see error: `You need a passphrase to unlock the secret key`.

    Run: `gpg -s aaa`

    `aaa` can be a non-exist file, and a window will pop up to let you enter the passphase.

    Run: `mvn deploy` again.

    If success, you will see the snapshot in `https://repository.apache.org/content/repositories/snapshots/org/apache/doris/je/`.

#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to build hadoop
##############################################################

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export SRC_HOME="${ROOT}"

. "${SRC_HOME}/env.sh"

if [[ -z "${THIRDPARTY_INSTALLED}" ]]; then
    echo "Must set 'THIRDPARTY_INSTALLED' in env.sh"
    exit -1
fi

DIST_DIR=${SRC_HOME}/hadoop-dist/target/hadoop-3.3.4/
LIBHDFS_DIST_DIR=${SRC_HOME}/hadoop-dist/target/hadoop-libhdfs-3.3.4/
rm -rf ${DIST_DIR}
rm -rf ${LIBHDFS_DIST_DIR}

export PATH=${THIRDPARTY_INSTALLED}/bin:$PATH
mvn clean package -Pnative,dist -DskipTests -Dthirdparty.installed=${THIRDPARTY_INSTALLED}/ -Dopenssl.lib=${THIRDPARTY_INSTALLED}/lib/ -e

if [[ ! -d "${DIST_DIR}" ]]; then
    echo "${DIST_DIR} is missing. Build failed."
    exit -1
fi

echo "Finished. Begin to pacakge for libhdfs..."
mkdir -p ${LIBHDFS_DIST_DIR}/common
mkdir -p ${LIBHDFS_DIST_DIR}/hdfs
mkdir -p ${LIBHDFS_DIST_DIR}/include
mkdir -p ${LIBHDFS_DIST_DIR}/native
cp -r ${DIST_DIR}/share/hadoop/common/* ${LIBHDFS_DIST_DIR}/common/
cp -r ${DIST_DIR}/share/hadoop/hdfs/* ${LIBHDFS_DIST_DIR}/hdfs/
cp -r ${DIST_DIR}/include/hdfs.h ${LIBHDFS_DIST_DIR}/include/
cp -r ${DIST_DIR}/lib/native/libhdfs.a ${LIBHDFS_DIST_DIR}/native/

echo "Done!"
echo "The full dist package is under: ${DIST_DIR}"
echo "The LIBHDFS dist package is under: ${LIBHDFS_DIST_DIR}"


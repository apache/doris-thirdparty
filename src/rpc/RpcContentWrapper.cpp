/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <google/protobuf/io/coded_stream.h>

#include "Exception.h"
#include "ExceptionInternal.h"
#include "RpcContentWrapper.h"

using namespace ::google::protobuf;
using namespace ::google::protobuf::io;

namespace Hdfs {
namespace Internal {

RpcContentWrapper::RpcContentWrapper(Message * header, Message * msg) :
    header(header), msg(msg) {
}

int RpcContentWrapper::getLength() {
    int headerLen, msgLen = 0;
    #if GOOGLE_PROTOBUF_VERSION >= 3010000
    size_t size_raw = header->ByteSizeLong();
    if (size_raw > INT_MAX) {
        THROW(HdfsIOException, "RpcContentWrapper::getLength: "
              "header size is too large: %zu", size_raw);
    }
    headerLen = static_cast<int>(size_raw);
    size_raw = msg == NULL ? 0 : msg->ByteSizeLong();;
    if (size_raw > INT_MAX) {
        THROW(HdfsIOException, "RpcContentWrapper::getLength: "
              "msg size is too large: %zu", size_raw);
    }
    msgLen = static_cast<int>(size_raw);
    #else
    headerLen = header->ByteSize());
    msgLen = msg == NULL ? 0 : msg->ByteSize();
    #endif
    return headerLen + CodedOutputStream::VarintSize32(headerLen)
           + (msg == NULL ?
              0 : msgLen + CodedOutputStream::VarintSize32(msgLen));
}

void RpcContentWrapper::writeTo(WriteBuffer & buffer) {
    #if GOOGLE_PROTOBUF_VERSION >= 3010000
    size_t size_raw = header->ByteSizeLong();
    if (size_raw > INT_MAX) {
        THROW(HdfsIOException, "RpcContentWrapper::writeTo: "
              "header size is too large: %zu", size_raw);
    }
    int headerLen = static_cast<int>(size_raw);
    #else
    int headerLen = header->ByteSize());
    #endif
    buffer.writeVarint32(headerLen);
    header->SerializeToArray(buffer.alloc(headerLen), headerLen);

    if (msg != NULL) {
        #if GOOGLE_PROTOBUF_VERSION >= 3010000
        size_t size_raw = msg->ByteSizeLong();
        if (size_raw > INT_MAX) {
            THROW(HdfsIOException, "RpcContentWrapper::writeTo: "
                "msg size is too large: %zu", size_raw);
        }
        int msgLen = static_cast<int>(size_raw);
        #else
        int msgLen = msg->ByteSize());
        #endif
        buffer.writeVarint32(msgLen);
        msg->SerializeToArray(buffer.alloc(msgLen), msgLen);
    }
}

}
}


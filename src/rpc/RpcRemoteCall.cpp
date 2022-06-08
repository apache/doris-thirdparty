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

#include "Exception.h"
#include "Memory.h"
#include "ProtobufRpcEngine.pb.h"
#include "RpcCall.h"
#include "RpcContentWrapper.h"
#include "RpcHeader.pb.h"
#include "RpcRemoteCall.h"
#include "WriteBuffer.h"

#include <google/protobuf/io/coded_stream.h>

#define PING_CALL_ID -4

using namespace google::protobuf::io;

namespace Hdfs {
namespace Internal {

void RpcRemoteCall::serialize(const RpcProtocolInfo & protocol,
                              WriteBuffer & buffer) {
    RpcRequestHeaderProto rpcHeader;
    rpcHeader.set_callid(identity);
    rpcHeader.set_clientid(clientId);
    rpcHeader.set_retrycount(-1);
    rpcHeader.set_rpckind(RPC_PROTOCOL_BUFFER);
    rpcHeader.set_rpcop(RpcRequestHeaderProto_OperationProto_RPC_FINAL_PACKET);
    RequestHeaderProto requestHeader;
    requestHeader.set_methodname(call.getName());
    requestHeader.set_declaringclassprotocolname(protocol.getProtocol());
    requestHeader.set_clientprotocolversion(protocol.getVersion());
    RpcContentWrapper wrapper(&requestHeader, call.getRequest());
    #if GOOGLE_PROTOBUF_VERSION >= 3010000
    size_t size_raw = rpcHeader.ByteSizeLong();
    if (size_raw > INT_MAX) {
        THROW(HdfsIOException, "RpcRemoteCall::serialize: rpcHeader message is too large: %zu", size_raw);
    }
    int rpcHeaderLen = static_cast<int>(size_raw);
    #else
    int rpcHeaderLen = rpcHeader.ByteSize());
    #endif
    int size = CodedOutputStream::VarintSize32(rpcHeaderLen) + rpcHeaderLen + wrapper.getLength();
    buffer.writeBigEndian(size);
    buffer.writeVarint32(rpcHeaderLen);
    rpcHeader.SerializeToArray(buffer.alloc(rpcHeaderLen), rpcHeaderLen);
    wrapper.writeTo(buffer);
}

std::vector<char> RpcRemoteCall::GetPingRequest(const std::string & clientid) {
    WriteBuffer buffer;
    std::vector<char> retval;
    RpcRequestHeaderProto pingHeader;
    pingHeader.set_callid(PING_CALL_ID);
    pingHeader.set_clientid(clientid);
    pingHeader.set_retrycount(INVALID_RETRY_COUNT);
    pingHeader.set_rpckind(RpcKindProto::RPC_PROTOCOL_BUFFER);
    pingHeader.set_rpcop(RpcRequestHeaderProto_OperationProto_RPC_FINAL_PACKET);
    #if GOOGLE_PROTOBUF_VERSION >= 3010000
    size_t size_raw = pingHeader.ByteSizeLong();
    if (size_raw > INT_MAX) {
        THROW(HdfsIOException, "RpcRemoteCall::GetPingRequest: pingHeader message is too large: %zu", size_raw);
    }
    int pingHeaderLen = static_cast<int>(size_raw);
    #else
    int pingHeaderLen = pingHeader.ByteSize());
    #endif
    int size = CodedOutputStream::VarintSize32(pingHeaderLen) + pingHeaderLen;
    buffer.writeBigEndian(size);
    buffer.writeVarint32(pingHeaderLen);
    pingHeader.SerializeWithCachedSizesToArray(reinterpret_cast<unsigned char *>(buffer.alloc(pingHeaderLen)));
    retval.resize(buffer.getDataSize(0));
    memcpy(&retval[0], buffer.getBuffer(0), retval.size());
    return retval;
}

}
}


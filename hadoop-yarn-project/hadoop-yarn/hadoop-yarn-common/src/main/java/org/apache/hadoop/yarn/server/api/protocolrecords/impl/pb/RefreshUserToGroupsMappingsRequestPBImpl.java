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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshUserToGroupsMappingsRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

@Private
@Unstable
public class RefreshUserToGroupsMappingsRequestPBImpl extends RefreshUserToGroupsMappingsRequest {

  private RefreshUserToGroupsMappingsRequestProto proto =
      RefreshUserToGroupsMappingsRequestProto.getDefaultInstance();
  private RefreshUserToGroupsMappingsRequestProto.Builder builder = null;
  private boolean viaProto = false;
  
  public RefreshUserToGroupsMappingsRequestPBImpl() {
    builder = RefreshUserToGroupsMappingsRequestProto.newBuilder();
  }

  public RefreshUserToGroupsMappingsRequestPBImpl(RefreshUserToGroupsMappingsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RefreshUserToGroupsMappingsRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {

    if (!(other instanceof RefreshUserToGroupsMappingsRequest)) {
      return false;
    }

    RefreshUserToGroupsMappingsRequestPBImpl otherImpl = this.getClass().cast(other);
    return new EqualsBuilder()
        .append(this.getProto(), otherImpl.getProto())
        .isEquals();
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RefreshUserToGroupsMappingsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getSubClusterId() {
    RefreshUserToGroupsMappingsRequestProtoOrBuilder p = viaProto ? proto : builder;
    boolean hasSubClusterId = p.hasSubClusterId();
    if (hasSubClusterId) {
      return p.getSubClusterId();
    }
    return null;
  }

  @Override
  public void setSubClusterId(String subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    builder.setSubClusterId(subClusterId);
  }
}
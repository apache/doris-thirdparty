// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License

#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "CLucene/SharedHeader.h"

CL_NS_DEF(index)

// Borrowed by IndexWriter for synchronous compaction. Sources/destinations are
// directory positions, not physical segment IDs. Terms may revisit lower doc IDs.
class CLUCENE_EXPORT DocIdMapping {
public:
    using Destination = std::pair<uint32_t, uint32_t>;
    virtual ~DocIdMapping() = default;
    virtual size_t sourceCount() const = 0;
    // UINT32_MAX in both fields means deleted. Lookup failures must throw
    // CLuceneError; treating a failure as a deleted document loses index entries.
    virtual Destination get(uint32_t source, uint32_t doc) const = 0;
};

CL_NS_END

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
// under the License.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cpuid.h>

size_t P4DEC(unsigned char *__restrict in, size_t n, uint32_t *__restrict out);
size_t P4NZDEC(unsigned char *__restrict in, size_t n, uint32_t *__restrict out);
size_t P4ENC(uint32_t *__restrict in, size_t n, unsigned char *__restrict out);
size_t P4NZENC(uint32_t *__restrict in, size_t n, unsigned char *__restrict out);


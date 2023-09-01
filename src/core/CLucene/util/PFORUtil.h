#pragma once

#include <cstddef>
#include <cstdint>
#include <cpuid.h>

size_t P4DEC(unsigned char *__restrict in, size_t n, uint32_t *__restrict out);
size_t P4NZDEC(unsigned char *__restrict in, size_t n, uint32_t *__restrict out);
size_t P4ENC(uint32_t *__restrict in, size_t n, unsigned char *__restrict out);
size_t P4NZENC(uint32_t *__restrict in, size_t n, unsigned char *__restrict out);


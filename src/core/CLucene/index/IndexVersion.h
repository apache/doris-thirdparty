#pragma once

enum class IndexVersion {
    kV0 = 0,
    kV1 = 1,    // Added frequency (freq) and position information
    kV2 = 2,    // Added PFOR compression for position information
    kV3 = 3,    // Applied ZSTD compression to the dictionary
    kV4 = 4,    // Added max block freq and norm

    kNone
};
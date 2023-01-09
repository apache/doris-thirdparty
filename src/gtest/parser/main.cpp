#include <cstdio>
#include "gtest/gtest.h"
#include "CLucene/analysis/mmseg/MMsegAnalyzer.h"

int main(int argc, char **argv) {
    printf("Running main() from %s\n", __FILE__);

#ifndef NDEBUG
    ::testing::GTEST_FLAG(filter) = "ValueTypeParserTest.ipv4";
#endif

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

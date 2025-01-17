/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "test.h"

unittest tests[] = {
        {"analysis", testanalysis},
        {"analyzers", testanalyzers},
        {"analysis", teststandard95},
        {"document", testdocument},
        {"field", testField},
        {"bkd", testBKD},
        {"MSBRadixSorter",testMSBRadixSorter},
        {"strconvert", testStrConvert},
        {"searchRange", testSearchRange},
        {"MultiPhraseQuery", testMultiPhraseQuery},
        {"IndexCompaction", testIndexCompaction},
        {"testStringReader", testStringReader},
        {"IndexCompress", testIndexCompress},
        {"IndexCompressV3", testIndexCompressV3},
        {"ByteArrayDataInput", testByteArrayDataInputSuite},
        {"GrowableByteArrayDataOutput", testGrowableByteArrayDataOutputSuite},
        {"testICU", testICU},
#ifdef TEST_CONTRIB_LIBS
        {"chinese", testchinese},
#endif
        {"LastTest", NULL}};

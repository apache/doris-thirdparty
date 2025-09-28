#include <CLucene.h>
#include <stdio.h>

#include "CLucene/index/IndexVersion.h"
#include "CLucene/index/_SkipListReader.h"
#include "CLucene/index/_SkipListWriter.h"
#include "CLucene/store/RAMDirectory.h"
#include "test.h"

using namespace lucene::index;
using namespace lucene::store;

void testDefaultSkipListWriterMaxBlockParams(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();

    try {
        IndexOutput* freqOutput = dir->createOutput("freq.dat");
        IndexOutput* proxOutput = dir->createOutput("prox.dat");

        const int32_t skipInterval = 4;
        const int32_t numberOfSkipLevels = 3;
        const int32_t docCount = 1000;

        DefaultSkipListWriter* writer =
                _CLNEW DefaultSkipListWriter(skipInterval, numberOfSkipLevels, docCount, freqOutput,
                                             proxOutput, IndexVersion::kV4);

        writer->resetSkip();

        for (int32_t doc = 4; doc <= 1000; doc += 4) {
            writer->setSkipData(doc, false, 0, doc, doc);

            writer->bufferSkip(doc);
        }

        int64_t skipPointer = writer->writeSkip(freqOutput);

        freqOutput->close();
        proxOutput->close();
        _CLDELETE(freqOutput);
        _CLDELETE(proxOutput);

        IndexInput* freqInput = nullptr;
        CLuceneError error;
        dir->openInput("freq.dat", freqInput, error);

        DefaultSkipListReader* reader = _CLNEW DefaultSkipListReader(
                freqInput, numberOfSkipLevels, skipInterval, IndexVersion::kV4);

        reader->init(skipPointer, 0, 0, docCount, true, false);

        for (int32_t doc = 4; doc <= 1000; doc += 4) {
            int32_t result = reader->skipTo(doc);

            int32_t actualMaxBlockFreq = reader->getMaxBlockFreq();
            int32_t actualMaxBlockNorm = reader->getMaxBlockNorm();

            CuAssertTrue(tc, actualMaxBlockFreq == doc);
            CuAssertTrue(tc, actualMaxBlockNorm == doc);
        }

        _CLDELETE(reader);
        _CLDELETE(writer);

    } catch (CLuceneError& e) {
        std::cout << e.what() << std::endl;
        CuFail(tc, e);
    }

    dir->close();
    _CLDELETE(dir);
}

void testDefaultSkipListWriterMaxBlockParamsV1(CuTest* tc) {
    RAMDirectory* dir = _CLNEW RAMDirectory();

    try {
        IndexOutput* freqOutput = dir->createOutput("freq.dat");
        IndexOutput* proxOutput = dir->createOutput("prox.dat");

        const int32_t skipInterval = 4;
        const int32_t numberOfSkipLevels = 3;
        const int32_t docCount = 1000;

        DefaultSkipListWriter* writer =
                _CLNEW DefaultSkipListWriter(skipInterval, numberOfSkipLevels, docCount, freqOutput,
                                             proxOutput, IndexVersion::kV1);

        writer->resetSkip();

        for (int32_t doc = 4; doc <= 1000; doc += 4) {
            writer->setSkipData(doc, false, 0, doc, doc);

            writer->bufferSkip(doc);
        }

        int64_t skipPointer = writer->writeSkip(freqOutput);

        freqOutput->close();
        proxOutput->close();
        _CLDELETE(freqOutput);
        _CLDELETE(proxOutput);

        IndexInput* freqInput = nullptr;
        CLuceneError error;
        dir->openInput("freq.dat", freqInput, error);

        DefaultSkipListReader* reader = _CLNEW DefaultSkipListReader(
                freqInput, numberOfSkipLevels, skipInterval, IndexVersion::kV1);

        reader->init(skipPointer, 0, 0, docCount, true, false);

        for (int32_t doc = 4; doc <= 1000; doc += 4) {
            int32_t result = reader->skipTo(doc);

            int32_t actualMaxBlockFreq = reader->getMaxBlockFreq();
            int32_t actualMaxBlockNorm = reader->getMaxBlockNorm();

            CuAssertTrue(tc, actualMaxBlockFreq == 0);
            CuAssertTrue(tc, actualMaxBlockNorm == 0);
        }

        _CLDELETE(reader);
        _CLDELETE(writer);

    } catch (CLuceneError& e) {
        std::cout << e.what() << std::endl;
        CuFail(tc, e);
    }

    dir->close();
    _CLDELETE(dir);
}

CuSuite* testBlockMaxScoreV3(void) {
    CuSuite* suite = CuSuiteNew(_T("CLucene BlockMaxScoreV3 Test"));

    SUITE_ADD_TEST(suite, testDefaultSkipListWriterMaxBlockParams);
    SUITE_ADD_TEST(suite, testDefaultSkipListWriterMaxBlockParamsV1);

    return suite;
}
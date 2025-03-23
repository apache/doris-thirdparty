//
// Created by 姜凯 on 2022/9/16.
//

#include "SDocumentWriter.h"
#include "IndexWriter.h"

#include "CLucene/analysis/AnalysisHeader.h"
#include "CLucene/document/Document.h"
#include "CLucene/search/Similarity.h"
#include "CLucene/util/CLStreams.h"
#include "CLucene/util/Misc.h"
#include "CLucene/util/stringUtil.h"
#include "CLucene/util/PFORUtil.h"
#include "CLucene/index/CodeMode.h"

#include "_FieldsWriter.h"
#include "_TermInfosWriter.h"
#include "_SkipListWriter.h"
#include "_IndexFileNames.h"
#include "_SegmentMerger.h"

#include <algorithm>
#include <vector>
#include <iostream>

CL_NS_USE(util)
CL_NS_USE(store)
CL_NS_USE(analysis)
CL_NS_USE(document)
CL_NS_DEF(index)

template<typename T>
const uint8_t SDocumentsWriter<T>::defaultNorm = search::Similarity::encodeNorm(1.0f);

template<typename T>
const int32_t SDocumentsWriter<T>::BYTE_BLOCK_SHIFT = 15;
template<typename T>
const int32_t SDocumentsWriter<T>::BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT;//(int32_t) pow(2.0, BYTE_BLOCK_SHIFT);
template<typename T>
const int32_t SDocumentsWriter<T>::BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;
template<typename T>
const int32_t SDocumentsWriter<T>::BYTE_BLOCK_NOT_MASK = ~BYTE_BLOCK_MASK;
template<typename T>
const int32_t SDocumentsWriter<T>::CHAR_BLOCK_SHIFT = 14;
template<typename T>
const int32_t SDocumentsWriter<T>::CHAR_BLOCK_SIZE = (int32_t) pow(2.0, CHAR_BLOCK_SHIFT);
template<typename T>
const int32_t SDocumentsWriter<T>::CHAR_BLOCK_MASK = CHAR_BLOCK_SIZE - 1;
template<typename T>
const int32_t SDocumentsWriter<T>::nextLevelArray[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};
template<typename T>
const int32_t SDocumentsWriter<T>::levelSizeArray[10] = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};
template<typename T>
const int32_t SDocumentsWriter<T>::POINTER_NUM_BYTE = 4;
template<typename T>
const int32_t SDocumentsWriter<T>::INT_NUM_BYTE = 4;
template<typename T>
const int32_t SDocumentsWriter<T>::CHAR_NUM_BYTE = 2;//TODO: adjust for c++...
template<typename T>
const int32_t SDocumentsWriter<T>::SCHAR_NUM_BYTE = 1;//TODO: adjust for c++...
template<typename T>
int32_t SDocumentsWriter<T>::OBJECT_HEADER_BYTES = 8;
template<typename T>
const int32_t SDocumentsWriter<T>::POSTING_NUM_BYTE = OBJECT_HEADER_BYTES + 9 * INT_NUM_BYTE + 5 * POINTER_NUM_BYTE;

template<typename T>
SDocumentsWriter<T>::ThreadState::ThreadState(SDocumentsWriter *p) : postingsFreeListTS(ValueArray<Posting *>(256)),
                                                                            vectorFieldPointers(ValueArray<int64_t>(10)),
                                                                            vectorFieldNumbers(ValueArray<int32_t>(10)),
                                                                            fieldDataArray(ValueArray<FieldData *>(8)),
                                                                            fieldDataHash(ValueArray<FieldData *>(16)),
                                                                            postingsPool(_CLNEW ByteBlockPool(true, p)),
                                                                            scharPool(_CLNEW SCharBlockPool(p)),
                                                                            allFieldDataArray(ValueArray<FieldData *>(10)),
                                                                            _parent(p) {
    fieldDataHashMask = 15;
    postingsFreeCountTS = 0;
    stringReader = _CLNEW ReusableStringReader(_T(""), 0, false);

    numThreads = 1;
    this->docBoost = 0.0;
    this->fieldGen = this->posUpto = this->numStoredFields = 0;
    this->numAllFieldData = this->docID = 0;
    this->numFieldData = numVectorFields = this->proxUpto = this->freqUpto = this->offsetUpto = 0;
    this->maxTermPrefix = nullptr;
    this->p = nullptr;
    this->prox = nullptr;
    this->offsets = nullptr;
    this->pos = nullptr;
    this->freq = nullptr;
    this->doFlushAfter = false;
}

template<typename T>
SDocumentsWriter<T>::~SDocumentsWriter() {
    if (skipListWriter!=nullptr) {
        _CLDELETE(skipListWriter);
    }
    if (fieldInfos != nullptr) {
        _CLDELETE(fieldInfos);
    }
    if (threadState != nullptr) {
        _CLDELETE(threadState);
    }
    if (_files != nullptr) {
        _CLDELETE(_files);
    }

    // Make sure unused posting slots aren't attempted delete on
    if (this->postingsFreeListDW.values) {
        if (this->postingsFreeCountDW < this->postingsFreeListDW.length) {
            memset(this->postingsFreeListDW.values + this->postingsFreeCountDW, 0, sizeof(Posting *));
        }
        postingsFreeListDW.deleteUntilNULL();
    }
}

template<typename T>
SDocumentsWriter<T>::ThreadState::~ThreadState() {
    _CLDELETE(postingsPool);
    _CLDELETE(scharPool);
    _CLDELETE(stringReader);

    for (size_t i = 0; i < allFieldDataArray.length; i++)
        _CLDELETE(allFieldDataArray.values[i]);
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::resetCurrentFieldData(Document *doc) {
    const Document::FieldsType &docFields = *doc->getFields();
    const int32_t numDocFields = docFields.size();

    if (FieldData* fp = fieldDataArray.values[0]; fp && numDocFields > 0) {
        numFieldData = 1;
        // reset fp for new fields
        fp->fieldCount = 0;
	// delete values is not make length reset to 0, so resize can not make sure new docFields values
        fp->docFields.deleteValues();
	fp->docFields.length = 0;
        fp->docFields.resize(1);
        for (int32_t i = 0; i < numDocFields; i++) {
            Field *field = docFields[i];
            if (fp->fieldCount == fp->docFields.length) {
                fp->docFields.resize(fp->docFields.length * 2);
            }

            fp->docFields.values[fp->fieldCount++] = field;
        }
    }
    return;
}

template<typename T>
typename SDocumentsWriter<T>::ThreadState *SDocumentsWriter<T>::getThreadState(Document *doc) {
    if (threadState == nullptr) {
        threadState = _CLNEW ThreadState(this);
    }

    if (segment.empty()) {
        segment = writer->newSegmentName();
        threadState->init(doc, nextDocID);
    } else if (doc->getNeedResetFieldData()) {
        threadState->resetCurrentFieldData(doc);
    }

    threadState->docID = nextDocID;

    // Only increment nextDocID & numDocsInRAM on successful init
    nextDocID++;
    numDocsInRAM++;
    // We must at this point commit to flushing to ensure we
    // always get N docs when we flush by doc count, even if
    // > 1 thread is adding documents:
    if (!flushPending && maxBufferedDocs != IndexWriter::DISABLE_AUTO_FLUSH && numDocsInRAM >= maxBufferedDocs) {
        flushPending = true;
        threadState->doFlushAfter = true;
    }

    return threadState;
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::init(Document *doc, int32_t doc_id) {
    this->docID = doc_id;
    docBoost = doc->getBoost();
    numStoredFields = 0;
    numFieldData = 0;
    numVectorFields = 0;
    maxTermPrefix = nullptr;

    const int32_t thisFieldGen = fieldGen++;

    const Document::FieldsType &docFields = *doc->getFields();
    const int32_t numDocFields = docFields.size();

    for (int32_t i = 0; i < numDocFields; i++) {
        Field *field = docFields[i];

        FieldInfo *fi = _parent->fieldInfos->add(field->name(), field->isIndexed(), field->isTermVectorStored(),
                                                 field->isStorePositionWithTermVector(), field->isStoreOffsetWithTermVector(),
                                                 field->getOmitNorms(), !field->getOmitTermFreqAndPositions(), false);
        fi->setIndexVersion(field->getIndexVersion());
        if (fi->isIndexed && !fi->omitNorms) {
            // Maybe grow our buffered norms
            if (_parent->norms.length <= fi->number) {
                auto newSize = (int32_t) ((1 + fi->number) * 1.25);
                _parent->norms.resize(newSize);
            }

            if (_parent->norms[fi->number] == NULL)
                _parent->norms.values[fi->number] = _CLNEW BufferedNorms();

            _parent->hasNorms = true;
        }

        // Make sure we have a FieldData allocated
        int32_t hashPos = Misc::thashCode(fi->name) & fieldDataHashMask;//TODO: put hash in fieldinfo
        FieldData *fp = fieldDataHash[hashPos];
        while (fp != nullptr && _tcscmp(fp->fieldInfo->name, fi->name) != 0)
            fp = fp->next;

        if (fp == nullptr) {
            fp = _CLNEW FieldData(_parent, this, fi);
            fp->next = fieldDataHash[hashPos];
            fieldDataHash.values[hashPos] = fp;

            if (numAllFieldData == allFieldDataArray.length) {
                allFieldDataArray.resize((int32_t) (allFieldDataArray.length * 1.5));

                ValueArray<FieldData *> newHashArray(fieldDataHash.length * 2);

                // Rehash
                fieldDataHashMask = allFieldDataArray.length - 1;
                for (size_t j = 0; j < fieldDataHash.length; j++) {
                    FieldData *fp0 = fieldDataHash[j];
                    while (fp0 != nullptr) {
                        //todo: put hash code into fieldinfo to reduce number of hashes necessary
                        hashPos = Misc::thashCode(fp0->fieldInfo->name) & fieldDataHashMask;
                        FieldData *nextFP0 = fp0->next;
                        fp0->next = newHashArray[hashPos];
                        newHashArray.values[hashPos] = fp0;
                        fp0 = nextFP0;
                    }
                }
                fieldDataHash.resize(newHashArray.length);
                memcpy(fieldDataHash.values, newHashArray.values, newHashArray.length * sizeof(FieldData *));
            }
            allFieldDataArray.values[numAllFieldData++] = fp;
        } else {
            assert(fp->fieldInfo == fi);
        }

        if (thisFieldGen != fp->lastGen) {

            // First time we're seeing this field for this doc
            fp->lastGen = thisFieldGen;
            fp->fieldCount = 0;
            fp->doNorms = fi->isIndexed && !fi->omitNorms;

            if (numFieldData == fieldDataArray.length) {
                fieldDataArray.resize(fieldDataArray.length * 2);
            }
            fieldDataArray.values[numFieldData++] = fp;
        }

        if (fp->fieldCount == fp->docFields.length) {
            fp->docFields.resize(fp->docFields.length * 2);
        }

        // Lazily allocate arrays for postings:
        if (field->isIndexed() && fp->postingsHash.values == nullptr)
            fp->initPostingArrays();

        fp->docFields.values[fp->fieldCount++] = field;
    }
    _parent->hasProx_ = _parent->fieldInfos->hasProx();
    _parent->indexVersion_ = _parent->fieldInfos->getIndexVersion();
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::writeDocument() {

    // If we hit an exception while appending to the
    // stored fields or term vectors files, we have to
    // abort all documents since we last flushed because
    // it means those files are possibly inconsistent.
    try {
        //_parent->numDocsInStore++;

        // Append norms for the fields we saw:
        for (int32_t i = 0; i < numFieldData; i++) {
            FieldData *fp = fieldDataArray[i];
            if (fp->doNorms) {
                BufferedNorms *bn = _parent->norms[fp->fieldInfo->number];
                assert(bn != nullptr);
                assert(bn->upto <= docID);
                bn->fill(docID);
                float_t norm = fp->boost * _parent->writer->getSimilarity()->lengthNorm(fp->fieldInfo->name, fp->length);
                bn->add(norm);
            }
        }
    } catch (CLuceneError &t) {
        throw;
    }

    if (_parent->bufferIsFull && !_parent->flushPending) {
        _parent->flushPending = true;
        doFlushAfter = true;
    }
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::FieldData::initPostingArrays() {
    // Target hash fill factor of <= 50%
    // NOTE: must be a power of two for hash collision
    // strategy to work correctly
    postingsHashSize = 4;
    postingsHashHalfSize = 2;
    postingsHashMask = postingsHashSize - 1;
    postingsHash.resize(postingsHashSize);
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::FieldData::processField(Analyzer *sanalyzer) {
    length = 0;
    position = 0;
    offset = 0;
    boost = threadState->docBoost;

    const int32_t maxFieldLength = _parent->writer->getMaxFieldLength();

    const int32_t limit = fieldCount;
    const ArrayBase<Field *> &docFieldsFinal = docFields;

    try {
        for (int32_t j = 0; j < limit; j++) {
            Field *field = docFieldsFinal[j];

            if (field->isIndexed()) {
                invertField(field, sanalyzer, maxFieldLength);
            }

            if (field->isStored()) {
                threadState->numStoredFields++;
            }

            // docFieldsFinal.values[j] = NULL;
        }
    } catch (CLuceneError& ae) {
        throw ae;
    }
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::FieldData::rehashPostings(const int32_t newSize) {
    const int32_t newMask = newSize - 1;

    ValueArray<Posting *> newHash(newSize);
    int32_t hashPos, code;
    const T *_pos = nullptr;
    const T *start = nullptr;
    Posting *p0;

    for (int32_t i = 0; i < postingsHashSize; i++) {
        p0 = postingsHash[i];
        if (p0 != nullptr) {
            start = threadState->scharPool->buffers[p0->textStart >> CHAR_BLOCK_SHIFT] + (p0->textStart & CHAR_BLOCK_MASK);
            _pos = start;
            while (*_pos != CLUCENE_END_OF_WORD)
                _pos++;
            code = 0;
            while (_pos > start)
                code = (code * 31) + *start++;
                //code = (code * 31) + *--_pos;

            hashPos = code & newMask;
            assert(hashPos >= 0);
            if (newHash[hashPos] != NULL) {
                const int32_t inc = ((code >> 8) + code) | 1;
                do {
                    code += inc;
                    hashPos = code & newMask;
                } while (newHash[hashPos] != NULL);
            }
            newHash.values[hashPos] = p0;
        }
    }

    postingsHashMask = newMask;
    postingsHash.deleteArray();
    postingsHash.length = newHash.length;
    postingsHash.values = newHash.takeArray();
    postingsHashSize = newSize;
    postingsHashHalfSize = newSize >> 1;
}

template<typename T>
bool SDocumentsWriter<T>::ThreadState::postingEquals(const T *tokenText, const int32_t tokenTextLen) {
    if (p->textLen != tokenTextLen) {
        return false;
    }
    const T *text = scharPool->buffers[p->textStart >> CHAR_BLOCK_SHIFT];
    assert(text != nullptr);
    int32_t position = p->textStart & CHAR_BLOCK_MASK;

    int32_t tokenPos = 0;
    for (; tokenPos < tokenTextLen; position++, tokenPos++) {
        if (tokenText[tokenPos] != text[position])
            return false;
    }
    return CLUCENE_END_OF_WORD == text[position];
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::writeFreqVInt(int32_t vi) {
    uint32_t i = vi;
    while ((i & ~0x7F) != 0) {
        writeFreqByte((uint8_t) ((i & 0x7f) | 0x80));
        i >>= 7;//unsigned shift...
    }
    writeFreqByte((uint8_t) i);
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::writeProxVInt(int32_t vi) {
    uint32_t i = vi;
    while ((i & ~0x7F) != 0) {
        writeProxByte((uint8_t) ((i & 0x7f) | 0x80));
        i >>= 7;//unsigned shift...
    }
    writeProxByte((uint8_t) i);
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::writeFreqByte(uint8_t b) {
    assert(freq != nullptr);
    if (freq[freqUpto] != 0) {
        freqUpto = postingsPool->allocSlice(freq, freqUpto);
        freq = postingsPool->buffer;
        p->freqUpto = postingsPool->tOffset;
    }
    freq[freqUpto++] = b;
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::writeProxByte(uint8_t b) {
    assert(prox != nullptr);
    if (prox[proxUpto] != 0) {
        proxUpto = postingsPool->allocSlice(prox, proxUpto);
        prox = postingsPool->buffer;
        p->proxUpto = postingsPool->tOffset;
        assert(prox != nullptr);
    }
    prox[proxUpto++] = b;
    assert(proxUpto != SDocumentsWriter::BYTE_BLOCK_SIZE);
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::writeProxBytes(const uint8_t *b, int32_t offset, int32_t len) {
    const int32_t offsetEnd = offset + len;
    while (offset < offsetEnd) {
        if (prox[proxUpto] != 0) {
            // End marker
            proxUpto = postingsPool->allocSlice(prox, proxUpto);
            prox = postingsPool->buffer;
            //p->proxUpto = postingsPool->tOffset;
        }

        prox[proxUpto++] = b[offset++];
        assert(proxUpto != SDocumentsWriter::BYTE_BLOCK_SIZE);
    }
}

template <typename T>
inline bool eq(const std::basic_string_view<T>& a, const std::basic_string_view<T>& b) {
    if constexpr (std::is_same_v<T, char>) {
#if defined(__SSE2__)
        if (a.size() != b.size()) {
            return false;
        }
        return StringUtil::memequalSSE2Wide(a.data(), b.data(), a.size());
#endif
    }
    return a == b;
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::FieldData::addPosition(Token *token) {
    const T *tokenText = token->termBuffer<T>();
    const int32_t tokenTextLen = token->termLength<T>();
    std::basic_string_view<T> term(tokenText, tokenTextLen);
    // if constexpr (std::is_same_v<T, char>) {
    //     std::cout << term << std::endl;
    // }
    uint32_t code = 0;

    // Compute hashcode
    //int32_t downto = tokenTextLen;
    int32_t upto = 0;
    //while (downto > 0)
    while (upto < tokenTextLen && tokenText[upto] != CLUCENE_END_OF_WORD)
        code = (code * 31) + tokenText[upto++];
    uint32_t hashPos = code & postingsHashMask;

    assert(!postingsCompacted);

    // Locate Posting in hash
    threadState->p = postingsHash[hashPos];

    if (threadState->p != nullptr && !eq(threadState->p->term_, term)) {
        const uint32_t inc = ((code >> 8) + code) | 1;
        do {
            postingsHashConflicts++;
            code += inc;
            hashPos = code & postingsHashMask;
            threadState->p = postingsHash[hashPos];
        } while (threadState->p != nullptr && !eq(threadState->p->term_, term));
    }

    int32_t proxCode = 0;
    try {
        if (threadState->p != nullptr) {                             // term seen since last flush
            if (threadState->docID != threadState->p->lastDocID) {// term not yet seen in this doc
                // Now that we know doc freq for previous doc,
                // write it & lastDocCode
                threadState->freqUpto = threadState->p->freqUpto & BYTE_BLOCK_MASK;
                threadState->freq = threadState->postingsPool->buffers[threadState->p->freqUpto >> BYTE_BLOCK_SHIFT];

                if (fieldInfo->hasProx) {
                    if (1 == threadState->p->docFreq)
                        threadState->writeFreqVInt(threadState->p->lastDocCode | 1);
                    else {
                        threadState->writeFreqVInt(threadState->p->lastDocCode);
                        threadState->writeFreqVInt(threadState->p->docFreq);
                    }
                } else {
                    threadState->writeFreqVInt(threadState->p->lastDocCode | 1);
                }

                threadState->p->freqUpto = threadState->freqUpto + (threadState->p->freqUpto & BYTE_BLOCK_NOT_MASK);

                if (fieldInfo->hasProx) {
                    proxCode = position;
                    threadState->p->docFreq = 1;
                }

                // Store code so we can write this after we're
                // done with this new doc
                threadState->p->lastDocCode = (threadState->docID - threadState->p->lastDocID) << 1;
                threadState->p->lastDocID = threadState->docID;

            } else {// term already seen in this doc
                if (fieldInfo->hasProx) {
                    threadState->p->docFreq++;
                    proxCode = position - threadState->p->lastPosition;
                }
            }
        } else {// term not seen before
            if (0 == threadState->postingsFreeCountTS) {
                _parent->getPostings(threadState->postingsFreeListTS);
                threadState->postingsFreeCountTS = threadState->postingsFreeListTS.length;
            }

            const int32_t textLen1 = 1 + tokenTextLen;
            if (textLen1 + threadState->scharPool->tUpto > CHAR_BLOCK_SIZE) {
                if (textLen1 > CHAR_BLOCK_SIZE) {
                    std::string errmsg = "bytes can be at most " +
                                         std::to_string(CHAR_BLOCK_SIZE - 1) +
                                         " in length; got " + std::to_string(tokenTextLen);
                    _CLTHROWA(CL_ERR_MaxBytesLength, errmsg.c_str());
                }
                threadState->scharPool->nextBuffer();
            }
            T *text = threadState->scharPool->buffer;
            T *textUpto = text + threadState->scharPool->tUpto;

            // Pull next free Posting from free list
            threadState->p = threadState->postingsFreeListTS[--threadState->postingsFreeCountTS];
            assert(threadState->p != nullptr);
            threadState->p->textStart = textUpto + threadState->scharPool->tOffset - text;
            threadState->p->textLen = tokenTextLen;
            threadState->scharPool->tUpto += textLen1;

            memcpy(textUpto, tokenText, tokenTextLen * sizeof(T));
            threadState->p->term_ = std::basic_string_view<T>(textUpto, term.size());
            textUpto[tokenTextLen] = CLUCENE_END_OF_WORD;

            assert(postingsHash[hashPos] == NULL);

            postingsHash.values[hashPos] = threadState->p;
            numPostings++;

            if (numPostings == postingsHashHalfSize)
                rehashPostings(2 * postingsHashSize);

            // Init first slice for freq & prox streams
            const int32_t firstSize = levelSizeArray[0];

            const int32_t upto1 = threadState->postingsPool->newSlice(firstSize);
            threadState->p->freqStart = threadState->p->freqUpto = threadState->postingsPool->tOffset + upto1;

            if (fieldInfo->hasProx) {
                const int32_t upto2 = threadState->postingsPool->newSlice(firstSize);
                threadState->p->proxStart = threadState->p->proxUpto = threadState->postingsPool->tOffset + upto2;
            }

            threadState->p->lastDocCode = threadState->docID << 1;
            threadState->p->lastDocID = threadState->docID;

            if (fieldInfo->hasProx) {
                threadState->p->docFreq = 1;
                proxCode = position;
            }
        }

        threadState->proxUpto = threadState->p->proxUpto & BYTE_BLOCK_MASK;
        threadState->prox = threadState->postingsPool->buffers[threadState->p->proxUpto >> BYTE_BLOCK_SHIFT];
        assert(threadState->prox != nullptr);

        if (fieldInfo->hasProx) {
            threadState->writeProxVInt(proxCode << 1);
            threadState->p->proxUpto = threadState->proxUpto + (threadState->p->proxUpto & BYTE_BLOCK_NOT_MASK);
            threadState->p->lastPosition = position++;
        }
    } catch (CLuceneError &t) {
        throw;
    }
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::FieldData::invertField(Field *field, Analyzer *sanalyzer, const int32_t maxFieldLength) {

    if (length > 0)
        position += sanalyzer->getPositionIncrementGap(fieldInfo->name);

    if (!field->isTokenized()) {// un-tokenized field
        const T *stringValue = (T *) field->rawStringValue();
        const size_t valueLength = field->getFieldDataLength();
        Token *token = localSToken;
        token->clear();

        token->setText(stringValue, valueLength);
        token->setStartOffset(offset);
        token->setEndOffset(offset + valueLength);
        addPosition(token);
        offset += valueLength;
        length++;
    } else {// tokenized field
        TokenStream *stream;
        TokenStream *streamValue = field->tokenStreamValue();

        if (streamValue != nullptr)
            stream = streamValue;
        else {
            // not support by now
            _CLTHROWA(CL_ERR_IllegalArgument, "field must have only STokenStream");
        }

        // reset the TokenStream to the first token
        stream->reset();

        try {
            offsetEnd = offset - 1;
            for (;;) {
                Token *token = stream->next(localSToken);
                if (token == nullptr) break;
                position += (token->getPositionIncrement() - 1);
                addPosition(token);
                ++length;
            }
            offset = offsetEnd + 1;
        }
        _CLFINALLY(
                stream->close();//don't delete, this stream is re-used
        )
    }

    boost *= field->getBoost();
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::processDocument(Analyzer *sanalyzer) {

    const int32_t numFields = numFieldData;

    // We process the document one field at a time
    for (int32_t i = 0; i < numFields; i++)
        fieldDataArray[i]->processField(sanalyzer);
    if (_parent->ramBufferSize != -1 && _parent->numBytesUsed > 0.95 * _parent->ramBufferSize)
        _parent->balanceRAM();
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::trimFields() {
    int32_t upto = 0;
    for (int32_t i = 0; i < numAllFieldData; i++) {
        FieldData *fp = allFieldDataArray[i];
        if (fp->lastGen == -1) {
            // This field was not seen since the previous
            // flush, so, free up its resources now

            // Unhash
            const int32_t hashPos = Misc::thashCode(fp->fieldInfo->name) & fieldDataHashMask;
            FieldData *last = nullptr;
            FieldData *fp0 = fieldDataHash[hashPos];
            while (fp0 != fp) {
                last = fp0;
                fp0 = fp0->next;
            }
            assert(fp0 != nullptr);

            if (last == nullptr)
                fieldDataHash.values[hashPos] = fp->next;
            else
                last->next = fp->next;

            _CLDELETE(fp);
        } else {
            // Reset
            fp->lastGen = -1;
            allFieldDataArray.values[upto++] = fp;

            if (fp->numPostings > 0 && ((float_t) fp->numPostings) / fp->postingsHashSize < 0.2) {
                int32_t hashSize = fp->postingsHashSize;

                // Reduce hash so it's between 25-50% full
                while (fp->numPostings < (hashSize >> 1) && hashSize >= 2)
                    hashSize >>= 1;
                hashSize <<= 1;

                if (hashSize != fp->postingsHash.length)
                    fp->rehashPostings(hashSize);
            }
        }
    }
    //delete everything after up to in allFieldDataArray
    for (size_t i = upto; i < allFieldDataArray.length; i++) {
        allFieldDataArray[i] = NULL;
    }

    // If we didn't see any norms for this field since
    // last flush, free it
    for (size_t i = 0; i < _parent->norms.length; i++) {
        BufferedNorms *n = _parent->norms[i];
        if (n != nullptr && n->upto == 0) {
            _CLLDELETE(n);
            _parent->norms.values[i] = NULL;
        }
    }

    numAllFieldData = upto;
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::resetPostings() {
    fieldGen = 0;
    doFlushAfter = false;
    postingsPool->reset();
    scharPool->reset();
    _parent->recyclePostings(this->postingsFreeListTS, this->postingsFreeCountTS);
    this->postingsFreeCountTS = 0;
    for (int32_t i = 0; i < numAllFieldData; i++) {
        FieldData *fp = allFieldDataArray[i];
        fp->lastGen = -1;
        if (fp->numPostings > 0)
            fp->resetPostingArrays();
    }
}

template<typename T>
int32_t SDocumentsWriter<T>::ThreadState::comparePostings(Posting *p1, Posting *p2) {
    const T *pos1 = scharPool->buffers[p1->textStart >> CHAR_BLOCK_SHIFT] + (p1->textStart & CHAR_BLOCK_MASK);
    const T *pos2 = scharPool->buffers[p2->textStart >> CHAR_BLOCK_SHIFT] + (p2->textStart & CHAR_BLOCK_MASK);
    while (true) {
        const auto c1 = static_cast<typename std::make_unsigned<T>::type>(*pos1++);
        const auto c2 = static_cast<typename std::make_unsigned<T>::type>(*pos2++);
        if (c1 < c2)
            if (CLUCENE_END_OF_WORD == c2)
                return 1;
            else
                return -1;
        else if (c2 < c1)
            if (CLUCENE_END_OF_WORD == c1)
                return -1;
            else
                return 1;
        else if (CLUCENE_END_OF_WORD == c1)
            return 0;
    }
}

template<typename T>
void SDocumentsWriter<T>::ThreadState::quickSort(Posting **postings, int32_t lo, int32_t hi) {
    if (lo >= hi)
        return;

    int32_t mid = ((uint32_t) (lo + hi)) >> 1;//unsigned shift...

    if (comparePostings(postings[lo], postings[mid]) > 0) {
        Posting *tmp = postings[lo];
        postings[lo] = postings[mid];
        postings[mid] = tmp;
    }

    if (comparePostings(postings[mid], postings[hi]) > 0) {
        Posting *tmp = postings[mid];
        postings[mid] = postings[hi];
        postings[hi] = tmp;

        if (comparePostings(postings[lo], postings[mid]) > 0) {
            Posting *tmp2 = postings[lo];
            postings[lo] = postings[mid];
            postings[mid] = tmp2;
        }
    }

    int32_t left = lo + 1;
    int32_t right = hi - 1;

    if (left >= right)
        return;

    Posting *partition = postings[mid];

    for (;;) {
        while (comparePostings(postings[right], partition) > 0)
            --right;

        while (left < right && comparePostings(postings[left], partition) <= 0)
            ++left;

        if (left < right) {
            Posting *tmp = postings[left];
            postings[left] = postings[right];
            postings[right] = tmp;
            --right;
        } else {
            break;
        }
    }

    quickSort(postings, lo, left);
    quickSort(postings, left + 1, hi);
}

template<typename T>
void SDocumentsWriter<T>::finishDocument(ThreadState *state) {
    // Now write the indexed document to the real files.
    if (nextWriteDocID == state->docID) {
        // It's my turn, so write everything now:
        nextWriteDocID++;
        state->writeDocument();
    } else {
        // No other choices.
    }
}

template<typename T>
bool SDocumentsWriter<T>::updateDocument(Document *doc, Analyzer *sanalyzer) {
    ThreadState *state = getThreadState(doc);
    try {
        bool success = false;
        try {
            try {
                state->processDocument(sanalyzer);
            }
            _CLFINALLY(
                    finishDocument(state);)
            success = true;
        }
    _CLFINALLY(
                if (!success) {
                    // If this thread state had decided to flush, we
                    // must clear it so another thread can flush
                    if (state->doFlushAfter) {
                        state->doFlushAfter = false;
                        flushPending = false;
                    }
                })
    } catch (CLuceneError& ae) {
        throw ae;
    }

    return state->doFlushAfter;
}

template<typename T>
bool SDocumentsWriter<T>::updateNullDocument(Document *doc) {
    ThreadState *state = getThreadState(doc);
    if (nextWriteDocID == state->docID) {
        nextWriteDocID++;
    }
    return true;
}

template<>
TCHAR *SDocumentsWriter<TCHAR>::getSCharBlock() {
    const size_t size = freeSCharBlocks.size();
    TCHAR *c;
    if (0 == size) {
        numBytesAlloc += CHAR_BLOCK_SIZE * CHAR_NUM_BYTE;
        c = _CL_NEWARRAY(TCHAR, CHAR_BLOCK_SIZE);
        memset(c, 0, sizeof(TCHAR) * CHAR_BLOCK_SIZE);
    } else {
        c = *freeSCharBlocks.begin();
        freeSCharBlocks.remove(freeSCharBlocks.begin(), true);
    }
    numBytesUsed += CHAR_BLOCK_SIZE * CHAR_NUM_BYTE;
    return c;
}

template<>
char *SDocumentsWriter<char>::getSCharBlock() {
    const size_t size = freeSCharBlocks.size();
    char *c;
    if (0 == size) {
        numBytesAlloc += CHAR_BLOCK_SIZE * SCHAR_NUM_BYTE;
        balanceRAM();
        c = _CL_NEWARRAY(char, CHAR_BLOCK_SIZE);
        memset(c, 0, sizeof(char) * CHAR_BLOCK_SIZE);
    } else {
        c = *freeSCharBlocks.begin();
        freeSCharBlocks.remove(freeSCharBlocks.begin(), true);
    }
    numBytesUsed += CHAR_BLOCK_SIZE * SCHAR_NUM_BYTE;
    return c;
}

template<typename T>
void SDocumentsWriter<T>::recycleSCharBlocks(ArrayBase<T *> &blocks, int32_t start, int32_t numBlocks) {
    for (int32_t i = start; i < numBlocks; i++) {
        freeSCharBlocks.push_back(blocks[i]);
        blocks.values[i] = NULL;
    }
}

template<typename T>
void SDocumentsWriter<T>::getPostings(ValueArray<Posting *> &postings) {
    numBytesUsed += postings.length * POSTING_NUM_BYTE;
    int32_t numToCopy;
    if (this->postingsFreeCountDW < postings.length)
        numToCopy = this->postingsFreeCountDW;
    else
        numToCopy = postings.length;

    const int32_t start = this->postingsFreeCountDW - numToCopy;
    if (numToCopy > 0) {
        memcpy(postings.values, this->postingsFreeListDW.values + start, sizeof(Posting *) * numToCopy);
    }
    this->postingsFreeCountDW -= numToCopy;

    // Directly allocate the remainder if any
    if (numToCopy < postings.length) {
        const int32_t extra = postings.length - numToCopy;
        const int32_t newPostingsAllocCount = this->postingsAllocCountDW + extra;
        if (newPostingsAllocCount > this->postingsFreeListDW.length)
            this->postingsFreeListDW.resize((int32_t) (1.25 * newPostingsAllocCount));

        balanceRAM();
        for (size_t i = numToCopy; i < postings.length; i++) {
            postings.values[i] = _CLNEW Posting();
            numBytesAlloc += POSTING_NUM_BYTE;
            this->postingsAllocCountDW++;
        }
    }
}


template<typename T>
void SDocumentsWriter<T>::writeNorms(const std::string &segmentName, int32_t totalNumDoc) {
    IndexOutput *normsOut = directory->createOutput((segmentName + "." + IndexFileNames::NORMS_EXTENSION).c_str());

    try {
        normsOut->writeBytes(SegmentMerger::NORMS_HEADER, SegmentMerger::NORMS_HEADER_length);

        const int32_t numField = fieldInfos->size();

        for (int32_t fieldIdx = 0; fieldIdx < numField; fieldIdx++) {
            FieldInfo *fi = fieldInfos->fieldInfo(fieldIdx);
            if (fi->isIndexed && !fi->omitNorms) {
                BufferedNorms *n = norms[fieldIdx];
                int64_t v;
                if (n == nullptr)
                    v = 0;
                else {
                    v = n->out.getFilePointer();
                    n->out.writeTo(normsOut);
                    n->reset();
                }
                if (v < totalNumDoc)
                    fillBytes(normsOut, defaultNorm, (int32_t) (totalNumDoc - v));
            }
        }
    }
    _CLFINALLY(
            normsOut->close();
            _CLDELETE(normsOut);)
}

template<typename T>
void SDocumentsWriter<T>::writeSegment(std::vector<std::string> &flushedFiles) {
    assert(nextDocID == numDocsInRAM);

    const std::string segmentName = segment;

    auto *termsOut = _CLNEW STermInfosWriter<T>(directory, segmentName.c_str(), fieldInfos,
                                            writer->getTermIndexInterval());
    termsOut->setEnableCorrectTermWrite(writer->getEnableCorrectTermWrite());
    IndexOutput *freqOut = directory->createOutput((segmentName + ".frq").c_str());
    // TODO:add options in field index
    IndexOutput *proxOut = nullptr;
    if (hasProx_) {
        proxOut = directory->createOutput((segmentName + ".prx").c_str());
    }

    // Gather all FieldData's that have postings, across all
    // ThreadStates
    std::vector<typename ThreadState::FieldData *> allFields;
    ThreadState *state = threadState;
    state->trimFields();
    const int32_t numFields = state->numAllFieldData;
    for (int32_t j = 0; j < numFields; j++) {
        typename SDocumentsWriter::ThreadState::FieldData *fp = state->allFieldDataArray[j];
        if (fp->numPostings > 0)
            allFields.push_back(fp);
    }

    // Sort by field name
    std::sort(allFields.begin(), allFields.end(), ThreadState::FieldData::sort);
    const int32_t numAllFields = allFields.size();

    skipListWriter = _CLNEW DefaultSkipListWriter(termsOut->skipInterval,
                                                  termsOut->maxSkipLevels,
                                         numDocsInRAM, freqOut, proxOut);

    int32_t start = 0;
    while (start < numAllFields) {

        const TCHAR *fieldName = allFields[start]->fieldInfo->name;

        int32_t end = start + 1;
        while (end < numAllFields && _tcscmp(allFields[end]->fieldInfo->name, fieldName) == 0)
            end++;

        ValueArray<typename ThreadState::FieldData *> fields(end - start);
        for (int32_t i = start; i < end; i++)
            fields.values[i - start] = allFields[i];

        // If this field has postings then add them to the
        // segment
        appendPostings(&fields, termsOut, freqOut, proxOut);

        for (size_t i = 0; i < fields.length; i++)
            fields[i]->resetPostingArrays();

        start = end;
    }

    freqOut->close();
    _CLDELETE(freqOut);
    if (proxOut != nullptr) {
        proxOut->close();
        _CLDELETE(proxOut);
    }
    termsOut->close();
    _CLDELETE(termsOut);
    _CLDELETE(skipListWriter);

    // Record all files we have flushed
    flushedFiles.push_back(segmentFileName(IndexFileNames::FIELD_INFOS_EXTENSION));
    flushedFiles.push_back(segmentFileName(IndexFileNames::FREQ_EXTENSION));
    if (hasProx_) {
        flushedFiles.push_back(segmentFileName(IndexFileNames::PROX_EXTENSION));
    }
    flushedFiles.push_back(segmentFileName(IndexFileNames::TERMS_EXTENSION));
    flushedFiles.push_back(segmentFileName(IndexFileNames::TERMS_INDEX_EXTENSION));

    if (hasNorms) {
        writeNorms(segmentName, numDocsInRAM);
        flushedFiles.push_back(segmentFileName(IndexFileNames::NORMS_EXTENSION));
    }

    resetPostingsData();

    nextDocID = 0;
    nextWriteDocID = 0;
    numDocsInRAM = 0;

    // Maybe downsize this->postingsFreeListDW array
    if (this->postingsFreeListDW.length > 1.5 * this->postingsFreeCountDW) {
        int32_t newSize = this->postingsFreeListDW.length;
        while (newSize > 1.25 * this->postingsFreeCountDW) {
            newSize = (int32_t) (newSize * 0.8);
        }
        this->postingsFreeListDW.resize(newSize);
    }
}

template<typename T>
void SDocumentsWriter<T>::recyclePostings(ValueArray<Posting *> &postings, int32_t numPostings) {
    // Move all Postings from this ThreadState back to our
    // free list.  We pre-allocated this array while we were
    // creating Postings to make sure it's large enough
    assert(this->postingsFreeCountDW + numPostings <= this->postingsFreeListDW.length);
    if (numPostings > 0)
        memcpy(this->postingsFreeListDW.values + this->postingsFreeCountDW, postings.values, numPostings * sizeof(Posting *));
    this->postingsFreeCountDW += numPostings;
}

template<typename T>
int32_t SDocumentsWriter<T>::compareText(const T *text1, const T *text2) {
    int32_t pos1 = 0;
    int32_t pos2 = 0;
    while (true) {
        const T c1 = text1[pos1++];
        const T c2 = text2[pos2++];
        if (c1 < c2)
            if (CLUCENE_END_OF_WORD == c2)
                return 1;
            else
                return -1;
        else if (c2 < c1)
            if (CLUCENE_END_OF_WORD == c1)
                return -1;
            else
                return 1;
        else if (CLUCENE_END_OF_WORD == c1)
            return 0;
    }
}

template<typename T>
void SDocumentsWriter<T>::appendPostings(ArrayBase<typename ThreadState::FieldData *> *fields,
                                     STermInfosWriter<T> *termsOut,
                                     IndexOutput *freqOut,
                                     IndexOutput *proxOut) {

    const int32_t fieldNumber = (*fields)[0]->fieldInfo->number;
    int32_t numFields = fields->length;
    // In Doris, field number is always 1 by now.
    assert(numFields == 1);

    ObjectArray<FieldMergeState> mergeStatesData(numFields);
    ValueArray<FieldMergeState *> mergeStates(numFields);

    for (int32_t i = 0; i < numFields; i++) {
        FieldMergeState *fms = mergeStatesData.values[i] = _CLNEW FieldMergeState();
        fms->field = (*fields)[i];
        fms->postings = fms->field->sortPostings();

        assert(fms->field->fieldInfo == (*fields)[0]->fieldInfo);

        // Should always be true
        bool result = fms->nextTerm();
        assert(result);
    }
    memcpy(mergeStates.values, mergeStatesData.values, sizeof(FieldMergeState *) * numFields);

    const int32_t skipInterval = termsOut->skipInterval;
    auto currentFieldStorePayloads = false;

    ValueArray<FieldMergeState *> termStates(numFields);

    while (numFields > 0) {

        // Get the next term to merge
        termStates.values[0] = mergeStates[0];
        int32_t numToMerge = 1;

        for (int32_t i = 1; i < numFields; i++) {
            const T *text = mergeStates[i]->text;
            const int32_t textOffset = mergeStates[i]->textOffset;
            const int32_t cmp = compareText(text + textOffset, termStates[0]->text + termStates[0]->textOffset);

            if (cmp < 0) {
                termStates.values[0] = mergeStates[i];
                numToMerge = 1;
            } else if (cmp == 0)
                termStates.values[numToMerge++] = mergeStates[i];
        }

        int32_t df = 0;
        int32_t lastPayloadLength = -1;

        int32_t lastDoc = 0;

        const T *start = termStates[0]->text + termStates[0]->textOffset;
        const T *pos = start;
        while (*pos != CLUCENE_END_OF_WORD)
            pos++;

        int64_t freqPointer = freqOut->getFilePointer();
        int64_t proxPointer = 0;
        if (hasProx_) {
            proxPointer = proxOut->getFilePointer();
        }

        skipListWriter->resetSkip();

        // Now termStates has numToMerge FieldMergeStates
        // which all share the same term.  Now we must
        // interleave the docID streams.
        while (numToMerge > 0) {

            if ((++df % skipInterval) == 0) {
                pfor_encode(freqOut, docDeltaBuffer, freqBuffer, hasProx_);

                skipListWriter->setSkipData(lastDoc, currentFieldStorePayloads, lastPayloadLength);
                skipListWriter->bufferSkip(df);
            }

            FieldMergeState *minState = termStates[0];
            for (int32_t i = 1; i < numToMerge; i++)
                if (termStates[i]->docID < minState->docID)
                    minState = termStates[i];

            const int32_t doc = minState->docID;
            const int32_t termDocFreq = minState->termFreq;

            assert(doc < numDocsInRAM);
            assert(doc > lastDoc || df == 1);

            const int32_t newDocCode = (doc - lastDoc) << 1;
            lastDoc = doc;

            ByteSliceReader &prox = minState->prox;

            // Carefully copy over the prox + payload info,
            // changing the format to match Lucene's segment
            // format.

            docDeltaBuffer.push_back(doc);
            if (hasProx_) {
                for (int32_t j = 0; j < termDocFreq; j++) {
                    const int32_t code = prox.readVInt();
                    assert(0 == (code & 1));
                    proxOut->writeVInt(code >> 1);
                }
                freqBuffer.push_back(termDocFreq);
            }

            if (!minState->nextDoc()) {

                // Remove from termStates
                int32_t upto = 0;
                for (int32_t i = 0; i < numToMerge; i++)
                    if (termStates[i] != minState)
                        termStates.values[upto++] = termStates[i];
                numToMerge--;
                assert(upto == numToMerge);

                // Advance this state to the next term

                if (!minState->nextTerm()) {
                    // OK, no more terms, so remove from mergeStates
                    // as well
                    upto = 0;
                    for (int32_t i = 0; i < numFields; i++)
                        if (mergeStates[i] != minState) {
                            mergeStates.values[upto++] = mergeStates[i];
                        }
                    numFields--;
                    assert(upto == numFields);
                }
            }
        }

        assert(df > 0);

        // Done merging this term
        {
            freqOut->writeByte((char)CodeMode::kDefault);
            freqOut->writeVInt(docDeltaBuffer.size());
            uint32_t lastDoc = 0;
            for (int32_t i = 0; i < docDeltaBuffer.size(); i++) {
                uint32_t curDoc = docDeltaBuffer[i];
                if (hasProx_) {
                    uint32_t newDocCode = (curDoc - lastDoc) << 1;
                    lastDoc = curDoc;
                    uint32_t freq = freqBuffer[i];
                    if (1 == freq) {
                        freqOut->writeVInt(newDocCode | 1);
                    } else {
                        freqOut->writeVInt(newDocCode);
                        freqOut->writeVInt(freq);
                    }
                } else {
                    freqOut->writeVInt(curDoc - lastDoc);
                    lastDoc = curDoc;
                }
            }
            docDeltaBuffer.resize(0);
            freqBuffer.resize(0);
        }
        
        int64_t skipPointer = skipListWriter->writeSkip(freqOut);

        // Write term
        termInfo.set(df, freqPointer, proxPointer, (int32_t) (skipPointer - freqPointer));
        termsOut->add(fieldNumber, start, pos - start, &termInfo);
    }
}

template<typename T>
int32_t SDocumentsWriter<T>::flush(bool _closeDocStore) {

    if (segment.empty()) {
        // In case we are asked to flush an empty segment
        segment = writer->newSegmentName();
    }

    newFiles.clear();

    docStoreOffset = numDocsInStore;

    int32_t docCount;

    assert(numDocsInRAM > 0);

    bool success = false;

    try {
        fieldInfos->write(directory, (segment + ".fnm").c_str());

        docCount = numDocsInRAM;

        writeSegment(newFiles);//write new files directly...

        success = true;
    }
    _CLFINALLY(
            if (!success)
                    abort(nullptr);)

    return docCount;
}

template<typename T>
uint8_t *SDocumentsWriter<T>::getByteBlock(bool trackAllocations) {
    const int32_t size = freeByteBlocks.size();
    uint8_t *b;
    if (0 == size) {
        numBytesAlloc += BYTE_BLOCK_SIZE;
        balanceRAM();
        b = _CL_NEWARRAY(uint8_t, BYTE_BLOCK_SIZE);
        memset(b, 0, sizeof(uint8_t) * BYTE_BLOCK_SIZE);
    } else {
        b = *freeByteBlocks.begin();
        freeByteBlocks.remove(freeByteBlocks.begin(), true);
    }
    if (trackAllocations)
        numBytesUsed += BYTE_BLOCK_SIZE;
    return b;
}

template<typename T>
void SDocumentsWriter<T>::recycleBlocks(ArrayBase<uint8_t *> &blocks, int32_t start, int32_t end) {
    for (int32_t i = start; i < end; i++) {
        freeByteBlocks.push_back(blocks[i]);
        blocks[i] = NULL;
    }
}

template<typename T>
void SDocumentsWriter<T>::fillBytes(IndexOutput *out, uint8_t b, int32_t numBytes) {
    for (int32_t i = 0; i < numBytes; i++)
        out->writeByte(b);
}

template<typename T>
T *SDocumentsWriter<T>::SCharBlockPool::getNewBlock(bool) {
    return BlockPool<T>::parent->getSCharBlock();
}

template<typename T>
void SDocumentsWriter<T>::SCharBlockPool::reset() {
    BlockPool<T>::parent->recycleSCharBlocks(BlockPool<T>::buffers, 0, 1 + BlockPool<T>::bufferUpto);
    BlockPool<T>::bufferUpto = -1;
    BlockPool<T>::tUpto = BlockPool<T>::blockSize;
    BlockPool<T>::tOffset = -BlockPool<T>::blockSize;
}

template<typename T>
SDocumentsWriter<T>::ThreadState::FieldData::FieldData(SDocumentsWriter *p, ThreadState *t, FieldInfo *fieldInfo) : docFields(ValueArray<Field *>(1)),
                                                                                                                                       _parent(p),
                                                                                                                                       localSToken(_CLNEW Token) {
    this->fieldCount = this->postingsHashSize = this->postingsHashHalfSize = this->postingsVectorsUpto = 0;
    this->postingsHashMask = this->offsetEnd = 0;
    this->offsetStartCode = this->offsetStart = this->numPostings = this->position = this->length = this->offset = 0;
    this->boost = 0.0;
    this->next = nullptr;
    this->lastGen = -1;
    this->fieldInfo = fieldInfo;
    this->threadState = t;
    this->postingsCompacted = false;
}

template<typename T>
SDocumentsWriter<T>::ThreadState::FieldData::~FieldData() {
    _CLDELETE(localSToken);
}

template<typename T>
int32_t SDocumentsWriter<T>::ThreadState::FieldData::compareTo(NamedObject *o) {
    if (o->getObjectName() != FieldData::getClassName())
        return -1;
    return _tcscmp(fieldInfo->name, ((FieldData *) o)->fieldInfo->name);
}

template<typename T>
SDocumentsWriter<T>::BufferedNorms::BufferedNorms() {
    this->upto = 0;
}
template<typename T>
void SDocumentsWriter<T>::BufferedNorms::add(float_t norm) {
    uint8_t b = search::Similarity::encodeNorm(norm);
    out.writeByte(b);
    upto++;
}
template<typename T>
void SDocumentsWriter<T>::BufferedNorms::reset() {
    out.reset();
    upto = 0;
}
template<typename T>
void SDocumentsWriter<T>::BufferedNorms::fill(int32_t docID) {
    // Must now fill in docs that didn't have this
    // field.  Note that this is how norms can consume
    // tremendous storage when the docs have widely
    // varying different fields, because we are not
    // storing the norms sparsely (see LUCENE-830)
    if (upto < docID) {
        fillBytes(&out, defaultNorm, docID - upto);
        upto = docID;
    }
}

template<typename T>
void SDocumentsWriter<T>::balanceRAM() {
    if (ramBufferSize == IndexWriter::DISABLE_AUTO_FLUSH || bufferIsFull)
        return;

    // We free our allocations if we've allocated 5% over
    // our allowed RAM buffer
    const int64_t freeTrigger = (int64_t) (1.05 * ramBufferSize);
    const int64_t freeLevel = (int64_t) (0.95 * ramBufferSize);

    // We flush when we've used our target usage
    const int64_t flushTrigger = (int64_t) ramBufferSize;

    if (numBytesAlloc > freeTrigger) {
        if (infoStream != NULL)
            (*infoStream) << string("  RAM: now balance allocations: usedMB=") << toMB(numBytesUsed) + string(" vs trigger=") << toMB(flushTrigger) << string(" allocMB=") << toMB(numBytesAlloc) << string(" vs trigger=") << toMB(freeTrigger) << string(" postingsFree=") << toMB(this->postingsFreeCountDW * POSTING_NUM_BYTE) << string(" byteBlockFree=") << toMB(freeByteBlocks.size() * BYTE_BLOCK_SIZE) << string(" charBlockFree=") << toMB(freeSCharBlocks.size() * CHAR_BLOCK_SIZE * CHAR_NUM_BYTE) << string("\n");

        // When we've crossed 100% of our target Postings
        // RAM usage, try to free up until we're back down
        // to 95%
        const int64_t startBytesAlloc = numBytesAlloc;

        const int32_t postingsFreeChunk = (int32_t) (BYTE_BLOCK_SIZE / POSTING_NUM_BYTE);

        int32_t iter = 0;

        // We free equally from each pool in 64 KB
        // chunks until we are below our threshold
        // (freeLevel)

        while (numBytesAlloc > freeLevel) {
            if (0 == freeByteBlocks.size() && 0 == freeSCharBlocks.size() && 0 == this->postingsFreeCountDW) {
                // Nothing else to free -- must flush now.
                bufferIsFull = true;
                if (infoStream != NULL)
                    (*infoStream) << string("    nothing to free; now set bufferIsFull\n");
                break;
            }

            if ((0 == iter % 3) && freeByteBlocks.size() > 0) {
                freeByteBlocks.remove(freeByteBlocks.size() - 1);
                numBytesAlloc -= BYTE_BLOCK_SIZE;
            }

            if ((1 == iter % 3) && freeSCharBlocks.size() > 0) {
                freeSCharBlocks.remove(freeSCharBlocks.size() - 1);
                numBytesAlloc -= CHAR_BLOCK_SIZE * CHAR_NUM_BYTE;
            }

            if ((2 == iter % 3) && this->postingsFreeCountDW > 0) {
                int32_t numToFree;
                if (this->postingsFreeCountDW >= postingsFreeChunk)
                    numToFree = postingsFreeChunk;
                else
                    numToFree = this->postingsFreeCountDW;
                for (size_t i = this->postingsFreeCountDW - numToFree; i < this->postingsFreeCountDW; i++) {
                    _CLDELETE(this->postingsFreeListDW.values[i]);
                }
                this->postingsFreeCountDW -= numToFree;
                this->postingsAllocCountDW -= numToFree;
                numBytesAlloc -= numToFree * POSTING_NUM_BYTE;
            }

            iter++;
        }

        if (infoStream != NULL) {
            (*infoStream) << "    after free: freedMB=" + Misc::toString((float_t) ((startBytesAlloc - numBytesAlloc) / 1024.0 / 1024.0)) +
                                     " usedMB=" + Misc::toString((float_t) (numBytesUsed / 1024.0 / 1024.0)) +
                                     " allocMB=" + Misc::toString((float_t) (numBytesAlloc / 1024.0 / 1024.0))
                          << string("\n");
        }

    } else {
        // If we have not crossed the 100% mark, but have
        // crossed the 95% mark of RAM we are actually
        // using, go ahead and flush.  This prevents
        // over-allocating and then freeing, with every
        // flush.
        if (numBytesUsed > flushTrigger) {
            if (infoStream != NULL) {
                (*infoStream) << string("  RAM: now flush @ usedMB=") << Misc::toString((float_t) (numBytesUsed / 1024.0 / 1024.0)) << string(" allocMB=") << Misc::toString((float_t) (numBytesAlloc / 1024.0 / 1024.0)) << string(" triggerMB=") << Misc::toString((float_t) (flushTrigger / 1024.0 / 1024.0)) << string("\n");
            }

            bufferIsFull = true;
        }
    }
}

template<typename T>
bool SDocumentsWriter<T>::hasProx() {
    return fieldInfos->hasProx();
}

template class SDocumentsWriter<char>;
template class SDocumentsWriter<TCHAR>;
CL_NS_END

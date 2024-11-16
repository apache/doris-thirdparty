/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
* 
* Distributable under the terms of either the Apache License (Version 2.0) or 
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "CLucene/_ApiHeader.h"
#include "_SegmentHeader.h"

#include "CLucene/store/IndexInput.h"
#include "CLucene/index/CodeMode.h"
#include "CLucene/util/PFORUtil.h"
#include "Term.h"

#include <assert.h>
#include <memory>
#include <iostream>

CL_NS_DEF(index)

SegmentTermDocs::SegmentTermDocs(const SegmentReader *_parent) : parent(_parent), freqStream(_parent->freqStream->clone()),
                                                                 count(0), df(0), deletedDocs(_parent->deletedDocs), _doc(-1), _freq(0), skipInterval(_parent->tis->getSkipInterval()),
                                                                 maxSkipLevels(_parent->tis->getMaxSkipLevels()), skipListReader(NULL), freqBasePointer(0), proxBasePointer(0),
                                                                 skipPointer(0), haveSkipped(false), pointer(0), pointerMax(0), indexVersion_(_parent->_fieldInfos->getIndexVersion()),
                                                                 hasProx(_parent->_fieldInfos->hasProx()), buffer_(freqStream, hasProx, indexVersion_) {
    CND_CONDITION(_parent != NULL, "Parent is NULL");
    memset(docs,0,PFOR_BLOCK_SIZE*sizeof(int32_t));
    memset(freqs,0,PFOR_BLOCK_SIZE*sizeof(int32_t));
}

SegmentTermDocs::~SegmentTermDocs() {
    close();
}

TermPositions *SegmentTermDocs::__asTermPositions() {
    return NULL;
}

void SegmentTermDocs::setIoContext(const void* io_ctx) {
    if (freqStream) {
        freqStream->setIoContext(io_ctx);
    }
    io_ctx_ = io_ctx;
}

int32_t SegmentTermDocs::docFreq() {
    return df;
}

void SegmentTermDocs::seek(Term *term) {
    TermInfo *ti = parent->tis->get(term, io_ctx_);
    seek(ti, term);
    _CLDELETE(ti);
}

void SegmentTermDocs::seek(TermEnum *termEnum) {
    TermInfo *ti = NULL;
    Term *term = NULL;

    // use comparison of fieldinfos to verify that termEnum belongs to the same segment as this SegmentTermDocs
    if (termEnum->getObjectName() == SegmentTermEnum::getClassName() &&
        ((SegmentTermEnum *) termEnum)->fieldInfos == parent->_fieldInfos) {
        SegmentTermEnum *segmentTermEnum = (SegmentTermEnum *) termEnum;
        term = segmentTermEnum->term(false);
        ti = segmentTermEnum->getTermInfo();
    } else {
        term = termEnum->term(false);
        ti = parent->tis->get(term);
    }

    seek(ti, term);
    _CLDELETE(ti);
}
void SegmentTermDocs::seek(const TermInfo *ti, Term *term) {
    count = 0;
    FieldInfo *fi = parent->_fieldInfos->fieldInfo(term->field());
    currentFieldStoresPayloads = (fi != NULL) ? fi->storePayloads : false;
    // hasProx = (fi != nullptr) && fi->hasProx;
    if (ti == NULL) {
        df = 0;
    } else {// punt case
        df = ti->docFreq;
        _doc = -1;
        freqBasePointer = ti->freqPointer;
        proxBasePointer = ti->proxPointer;
        skipPointer = freqBasePointer + ti->skipOffset;
        freqStream->seek(freqBasePointer);
        haveSkipped = false;
    }
}

void SegmentTermDocs::close() {
    _CLDELETE(freqStream);
    _CLDELETE(skipListReader);
}

int32_t SegmentTermDocs::doc() const {
    return _doc;
}
int32_t SegmentTermDocs::freq() const {
    return _freq;
}

bool SegmentTermDocs::next()  {
    if (count == df) {
        _doc = LUCENE_INT32_MAX_SHOULDBE;
        return false;
    }

    _doc = buffer_.getDoc();
    if (hasProx) {
        _freq = buffer_.getFreq();
    }

    count++;

    return true;
}

int32_t SegmentTermDocs::read(int32_t *docs, int32_t *freqs, int32_t length) {
    int32_t i = 0;
    
    if (count == df) {
        return i;
    }

    while (i < length && count < df) {
        _doc = buffer_.getDoc();
        docs[i] = _doc;

        if (hasProx) {
            _freq = buffer_.getFreq();
            freqs[i] = _freq;
        }

        count++;
        i++;
    }

    return i;
}

bool SegmentTermDocs::readRange(DocRange* docRange) {
    if (count >= df) {
        return false;
    }

    buffer_.readRange(docRange);

    count += docRange->doc_many_size_;

    if (docRange->doc_many_size_ > 0) {
        uint32_t start = (*docRange->doc_many)[0];
        uint32_t end = (*docRange->doc_many)[docRange->doc_many_size_ - 1];
        if ((end - start) == docRange->doc_many_size_ - 1) {
            docRange->doc_range.first = start;
            docRange->doc_range.second = start + docRange->doc_many_size_;
            docRange->type_ = DocRangeType::kRange;
        }
    }

    return true;
}

bool SegmentTermDocs::skipTo(const int32_t target) {
    assert(count <= df);

    if (df >= skipInterval) {// optimized case
        if (skipListReader == NULL) {
            skipListReader = _CLNEW DefaultSkipListReader(freqStream->clone(), maxSkipLevels, skipInterval);// lazily clone
            skipListReader->setIoContext(io_ctx_);
        }

        if (!haveSkipped) {// lazily initialize skip stream
            skipListReader->init(skipPointer, freqBasePointer, proxBasePointer, df, hasProx, currentFieldStoresPayloads);
            haveSkipped = true;
        }

        int32_t newCount = skipListReader->skipTo(target);
        if (newCount > count) {
            freqStream->seek(skipListReader->getFreqPointer());
            skipProx(skipListReader->getProxPointer(), skipListReader->getPayloadLength());

            _doc = skipListReader->getDoc();
            count = newCount;
            buffer_.refill();
        }
    }

    // done skipping, now just scan
    do {
        if (!next())
            return false;
    } while (target > _doc);
    return true;
}

void TermDocsBuffer::refill() {
    cur_doc_ = 0;
    cur_freq_ = 0;

    if (indexVersion_ >= IndexVersion::kV1) {
        size_ = refillV1();
    } else {
        size_ = refillV0();
    }
}

void TermDocsBuffer::readRange(DocRange* docRange) {
    int32_t size = 0;
    if (indexVersion_ >= IndexVersion::kV1) {
        size = refillV1();
    } else {
        size = refillV0();
    }
    docRange->type_ = DocRangeType::kMany;
    docRange->doc_many = &docs_;
    docRange->doc_many_size_ = size;
    if (hasProx_) {
        docRange->freq_many = &freqs_;
        docRange->freq_many_size_ = size;
    }
}

int32_t TermDocsBuffer::refillV0() {
    if (hasProx_) {
        char mode = freqStream_->readByte();
        uint32_t arraySize = freqStream_->readVInt();
        if (mode == (char)CodeMode::kPfor) {
            {
                uint32_t SerializedSize = freqStream_->readVInt();
                std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
                freqStream_->readBytes(buf.data(), SerializedSize);
                P4DEC(buf.data(), arraySize, docs_.data());
            }
            {
                uint32_t SerializedSize = freqStream_->readVInt();
                std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
                freqStream_->readBytes(buf.data(), SerializedSize);
                P4NZDEC(buf.data(), arraySize, freqs_.data());
            }
        } else if (mode == (char)CodeMode::kDefault) {
            uint32_t docDelta = 0;
            for (uint32_t i = 0; i < arraySize; i++) {
                uint32_t docCode = freqStream_->readVInt();
                docDelta += (docCode >> 1);
                docs_[i] = docDelta;
                if ((docCode & 1) != 0) {
                    freqs_[i] = 1;
                } else {
                    freqs_[i] = freqStream_->readVInt();
                }
            }
        }
        return arraySize;
    } else {
        uint32_t arraySize = freqStream_->readVInt();
        if (arraySize < PFOR_BLOCK_SIZE) {
            uint32_t docDelta = 0;
            for (uint32_t i = 0; i < arraySize; i++) {
                uint32_t docCode = freqStream_->readVInt();
                docDelta += docCode;
                docs_[i] = docDelta;
            }
        } else {
            {
                uint32_t serializedSize = freqStream_->readVInt();
                std::vector<uint8_t> buf(serializedSize + PFOR_BLOCK_SIZE);
                freqStream_->readBytes(buf.data(), serializedSize);
                P4DEC(buf.data(), arraySize, docs_.data());
            }
        }
        return arraySize;
    }
}

int32_t TermDocsBuffer::refillV1() {
    char mode = freqStream_->readByte();
    uint32_t arraySize = freqStream_->readVInt();
    if (mode == (char)CodeMode::kPfor) {
        {
            uint32_t SerializedSize = freqStream_->readVInt();
            std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
            freqStream_->readBytes(buf.data(), SerializedSize);
            P4DEC(buf.data(), arraySize, docs_.data());
        }
        if (hasProx_) {
            uint32_t SerializedSize = freqStream_->readVInt();
            std::vector<uint8_t> buf(SerializedSize + PFOR_BLOCK_SIZE);
            freqStream_->readBytes(buf.data(), SerializedSize);
            P4NZDEC(buf.data(), arraySize, freqs_.data());
        }
    } else if (mode == (char)CodeMode::kDefault) {
        uint32_t docDelta = 0;
        for (uint32_t i = 0; i < arraySize; i++) {
            uint32_t docCode = freqStream_->readVInt();
            if (hasProx_) {
                docDelta += (docCode >> 1);
                docs_[i] = docDelta;
                if ((docCode & 1) != 0) {
                    freqs_[i] = 1;
                } else {
                    freqs_[i] = freqStream_->readVInt();
                }
            } else {
                docDelta += docCode;
                docs_[i] = docDelta;
            }            
        }
    }
    return arraySize;
}

CL_NS_END

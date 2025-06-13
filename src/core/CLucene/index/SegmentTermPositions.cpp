/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
* 
* Distributable under the terms of either the Apache License (Version 2.0) or 
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "CLucene/_ApiHeader.h"
#include "_SegmentHeader.h"

#include "Terms.h"

#include <iostream>

CL_NS_USE(util)
CL_NS_DEF(index)

SegmentTermPositions::SegmentTermPositions(const SegmentReader* _parent):
	SegmentTermDocs(_parent), proxStream(NULL)// the proxStream will be cloned lazily when nextPosition() is called for the first time
	,lazySkipPointer(-1), lazySkipProxCount(0)
    , indexVersion_(_parent->_fieldInfos->getIndexVersion())
    , buffer_(proxStream, indexVersion_)
{
    CND_CONDITION(_parent != NULL, "Parent is NULL");
}

SegmentTermPositions::~SegmentTermPositions() {
    close();
}

void SegmentTermPositions::setIoContext(const void* io_ctx) {
    SegmentTermDocs::setIoContext(io_ctx);
}

TermDocs* SegmentTermPositions::__asTermDocs(){
    return (TermDocs*) this;
}
TermPositions* SegmentTermPositions::__asTermPositions(){
    return (TermPositions*) this;
}

void SegmentTermPositions::seek(const TermInfo* ti, Term* term, bool local_stats) {
    SegmentTermDocs::seek(ti, term, local_stats);
    if (ti != NULL)
    	lazySkipPointer = ti->proxPointer;
    
    lazySkipProxCount = 0;
    proxCount = 0;
    payloadLength = 0;
    needToLoadPayload = false;
}

void SegmentTermPositions::close() {
    SegmentTermDocs::close();
    //Check if proxStream still exists
    if(proxStream){
        proxStream->close();
        _CLDELETE( proxStream );
    }
}

int32_t SegmentTermPositions::nextPosition() {
    // TODO:need to do like this:    if (indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
    if (!hasProx){
        return 0;
    }
    // perform lazy skips if neccessary
	lazySkip();
    proxCount--;
    return position += readDeltaPosition();
}

int32_t SegmentTermPositions::readDeltaPosition() {
	int32_t delta = buffer_.getPos();
	if (currentFieldStoresPayloads) {
		// if the current field stores payloads then
		// the position delta is shifted one bit to the left.
		// if the LSB is set, then we have to read the current
		// payload length
		if ((delta & 1) != 0) {
			// payloadLength = proxStream->readVInt();
            _CLTHROWA(CL_ERR_UnsupportedOperation, "Processing the payload flow is not supported at the moment");
		} 
		delta = (int32_t)((uint32_t)delta >> (uint32_t)1);
		needToLoadPayload = true;
	}
	return delta;
}

void SegmentTermPositions::skippingDoc() {
	lazySkipProxCount += _freq;
}

bool SegmentTermPositions::next() {
	// we remember to skip the remaining positions of the current
    // document lazily
    lazySkipProxCount += proxCount;

    if (SegmentTermDocs::next()) {				  // run super
        proxCount = _freq;				  // note frequency
        position = 0;				  // reset position
        return true;
    }
    return false;
}

int32_t SegmentTermPositions::read(int32_t* /*docs*/, int32_t* /*freqs*/, int32_t /*length*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,"TermPositions does not support processing multiple documents in one call. Use TermDocs instead.");
}

int32_t SegmentTermPositions::read(int32_t* /*docs*/, int32_t* /*freqs*/, int32_t*  /*norms*/, int32_t /*length*/) {
    _CLTHROWA(CL_ERR_UnsupportedOperation,"TermPositions does not support processing multiple documents in one call. Use TermDocs instead.");
}

bool SegmentTermPositions::readRange(DocRange* docRange) {
    return SegmentTermDocs::readRange(docRange);
}

void SegmentTermPositions::skipProx(const int64_t proxPointer, const int32_t _payloadLength){
    // we save the pointer, we might have to skip there lazily
    lazySkipPointer = proxPointer;
    lazySkipProxCount = 0;
    proxCount = 0;
    this->payloadLength = _payloadLength;
    needToLoadPayload = false;
}

void SegmentTermPositions::skipPositions(const int32_t n) {
	for ( int32_t f = n; f > 0; f-- ) {		// skip unread positions
		readDeltaPosition();
		skipPayload();
	}
}

void SegmentTermPositions::skipPayload() {
	if (needToLoadPayload && payloadLength > 0) {
		// proxStream->seek(proxStream->getFilePointer() + payloadLength);
        _CLTHROWA(CL_ERR_UnsupportedOperation, "Processing the payload flow is not supported at the moment");
	}
	needToLoadPayload = false;
}

void SegmentTermPositions::lazySkip() {
    if (proxStream == NULL) {
      // clone lazily
      proxStream = parent->proxStream->clone();
      proxStream->setIoContext(io_ctx_);
      buffer_.reset(proxStream);
    }
    
    // we might have to skip the current payload
    // if it was not read yet
    skipPayload();
      
    if (lazySkipPointer != -1) {
      buffer_.seek(lazySkipPointer);
      lazySkipPointer = -1;
    }
     
    if (lazySkipProxCount != 0) {
      skipPositions(lazySkipProxCount);
      lazySkipProxCount = 0;
    }
}

int32_t SegmentTermPositions::getPayloadLength() const { return payloadLength; }

uint8_t* SegmentTermPositions::getPayload(uint8_t* data) {
	if (!needToLoadPayload) {
		_CLTHROWA(CL_ERR_IO, "Payload cannot be loaded more than once for the same term position.");
	}

	// read payloads lazily
	uint8_t* retArray;
	// TODO: Complete length logic ( possibly using ValueArray ? )
	if (data == NULL /*|| data.length - offset < payloadLength*/) {
		// the array is too small to store the payload data,
		// so we allocate a new one
		_CLDELETE_ARRAY(data);
		retArray = _CL_NEWARRAY(uint8_t, payloadLength);
	} else {
		retArray = data;
	}
	// proxStream->readBytes(retArray, payloadLength);
    _CLTHROWA(CL_ERR_UnsupportedOperation, "Processing the payload flow is not supported at the moment");
	needToLoadPayload = false;
	return retArray;
}
bool SegmentTermPositions::isPayloadAvailable() const { return needToLoadPayload && (payloadLength > 0); }

CL_NS_END

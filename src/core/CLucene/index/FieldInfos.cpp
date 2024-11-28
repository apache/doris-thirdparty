/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
* 
* Distributable under the terms of either the Apache License (Version 2.0) or 
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include <cstdint>
#include "CLucene/_ApiHeader.h"
#include "_FieldInfos.h"

#include "CLucene/store/Directory.h"
#include "CLucene/document/Document.h"
#include "CLucene/document/Field.h"
////#include "CLucene/util/VoidMap.h"
#include "CLucene/util/Misc.h"
#include "CLucene/util/_StringIntern.h"
#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"

CL_NS_USE(store)
CL_NS_USE(document)
CL_NS_USE(util)
CL_NS_DEF(index)


FieldInfo::FieldInfo(const TCHAR *_fieldName,
                     const bool _isIndexed,
                     const int32_t _fieldNumber,
                     const bool _storeTermVector,
                     const bool _storeOffsetWithTermVector,
                     const bool _storePositionWithTermVector,
                     const bool _omitNorms,
										 const bool _hasProx,
                     const bool _storePayloads,
										 IndexVersion indexVersion,
										 uint32_t flags) : name(CLStringIntern::intern(_fieldName )),
                                                  isIndexed(_isIndexed),
                                                  number(_fieldNumber),
                                                  storeTermVector(_storeTermVector),
                                                  storeOffsetWithTermVector(_storeOffsetWithTermVector),
                                                  storePositionWithTermVector(_storePositionWithTermVector),
                                                  omitNorms(_omitNorms), hasProx(_hasProx),
																									storePayloads(_storePayloads),
																									indexVersion_(indexVersion), flags_(flags) {
}

FieldInfo::~FieldInfo(){
	CL_NS(util)::CLStringIntern::unintern(name);
}

FieldInfo* FieldInfo::clone() {
	return _CLNEW FieldInfo(name, isIndexed, number, storeTermVector, storePositionWithTermVector,
		storeOffsetWithTermVector, omitNorms, hasProx, storePayloads, indexVersion_, flags_);
}

FieldInfos::FieldInfos():
	byName(false,false),byNumber(true) {
}

FieldInfos::~FieldInfos(){
	byName.clear();
	byNumber.clear();
}

FieldInfos::FieldInfos(Directory* d, const char* name):
	byName(false,false),byNumber(true) 
{
	IndexInput* input = d->openInput(name);
	try {	
		read(input);
	} _CLFINALLY (
	    input->close();
	    _CLDELETE(input);
	);
}

FieldInfos* FieldInfos::clone()
{
	FieldInfos* fis = _CLNEW FieldInfos();
	const size_t numField = byNumber.size();
	for(size_t i=0;i<numField;i++) {
		FieldInfo* fi = byNumber[i]->clone();
		fis->byNumber.push_back(fi);
		fis->byName.put( fi->name, fi);
	}
	return fis;
}

void FieldInfos::add(const Document* doc) {
  const Document::FieldsType& fields = *doc->getFields();
	Field* field;
  for ( Document::FieldsType::const_iterator itr = fields.begin() ; itr != fields.end() ; itr++ ){
			field = *itr;
			add(field->name(), field->isIndexed(), field->isTermVectorStored(), field->isStorePositionWithTermVector(),
              field->isStoreOffsetWithTermVector(), field->getOmitNorms(), !field->getOmitTermFreqAndPositions(), false);
	}
}

bool FieldInfos::hasProx() {
	int numFields = byNumber.size();
	for (int i = 0; i < numFields; i++) {
		FieldInfo* fi = fieldInfo(i);
		if (fi->isIndexed && fi->hasProx) {
				return true;
		}
	}
	return false;
}

IndexVersion FieldInfos::getIndexVersion() {
	int numFields = byNumber.size();
	for (int i = 0; i < numFields; i++) {
		FieldInfo* fi = fieldInfo(i);
		if (fi->indexVersion_ > IndexVersion::kV0) {
			return fi->indexVersion_;
		}
	}
	return IndexVersion::kV0;
}

uint32_t FieldInfos::getFlags() {
	size_t numFields = byNumber.size();
	if (numFields > 0) {
		// Currently, only single-field configuration retrieval is supported.
		FieldInfo* fi = fieldInfo(0);
		return fi->getFlags();
	}
	return 0;
}

void FieldInfos::addIndexed(const TCHAR** names, const bool storeTermVectors, const bool storePositionWithTermVector,
							const bool storeOffsetWithTermVector) {
	size_t i = 0;
	while (names[i]) {
		add(names[i], true, storeTermVectors, storePositionWithTermVector, storeOffsetWithTermVector);
		++i;
	}
}

void FieldInfos::add(const TCHAR** names, const bool isIndexed, const bool storeTermVectors,
                     const bool storePositionWithTermVector, const bool storeOffsetWithTermVector,
                     const bool omitNorms, const bool hasProx, const bool storePayloads,
					 					 IndexVersion indexVersion, uint32_t flags) {
        size_t i = 0;      
	while ( names[i] != NULL ){
		add(names[i], isIndexed, storeTermVectors, storePositionWithTermVector, 
			storeOffsetWithTermVector, omitNorms, hasProx, storePayloads, indexVersion, flags);
		++i;
	}
}

FieldInfo* FieldInfos::add(const TCHAR* name, const bool isIndexed, const bool storeTermVector,
                           const bool storePositionWithTermVector,
                           const bool storeOffsetWithTermVector, const bool omitNorms,
                           const bool hasProx, const bool storePayloads,
						   						 IndexVersion indexVersion, uint32_t flags) {
  FieldInfo* fi = fieldInfo(name);
	if (fi == NULL) {
		return addInternal(name, isIndexed, storeTermVector, storePositionWithTermVector,
												storeOffsetWithTermVector, omitNorms, hasProx, storePayloads,
												indexVersion);
  } else {
		if (fi->isIndexed != isIndexed) {
			fi->isIndexed = true;                      // once indexed, always index
		}
		if (fi->storeTermVector != storeTermVector) {
			fi->storeTermVector = true;                // once vector, always vector
		}
		if (fi->storePositionWithTermVector != storePositionWithTermVector) {
	        fi->storePositionWithTermVector = true;                // once vector, always vector
	    }
	    if (fi->storeOffsetWithTermVector != storeOffsetWithTermVector) {
	        fi->storeOffsetWithTermVector = true;                // once vector, always vector
	    }
	    if (fi->omitNorms != omitNorms) {
	        fi->omitNorms = false;                // once norms are stored, always store
	    }
			if (fi->hasProx != hasProx) {
				fi->hasProx = true;
			}
		if (fi->storePayloads != storePayloads) {
			fi->storePayloads = true;
		}
		if (fi->indexVersion_ != indexVersion) {
			fi->indexVersion_ = indexVersion;
		}
		if (fi->flags_ != flags) {
			fi->flags_ = flags;
		}
	}
	return fi;
}

FieldInfo* FieldInfos::addInternal(const TCHAR* name, const bool isIndexed,
                                   const bool storeTermVector,
                                   const bool storePositionWithTermVector,
                                   const bool storeOffsetWithTermVector, const bool omitNorms,
                                   const bool hasProx, const bool storePayloads,
								   								 IndexVersion indexVersion, uint32_t flags) {
	FieldInfo* fi = _CLNEW FieldInfo(name, isIndexed, byNumber.size(), storeTermVector,
																		storePositionWithTermVector, storeOffsetWithTermVector,
																		omitNorms, hasProx, storePayloads, indexVersion, flags);
  byNumber.push_back(fi);
	byName.put( fi->name, fi);
	return fi;
}

int32_t FieldInfos::fieldNumber(const TCHAR* fieldName)const {
	FieldInfo* fi = fieldInfo(fieldName);
	return (fi!=NULL) ? fi->number : -1;
}

FieldInfo* FieldInfos::fieldInfo(const TCHAR* fieldName) const {
	FieldInfo* ret = byName.get(fieldName);
	return ret;
}

const TCHAR* FieldInfos::fieldName(const int32_t fieldNumber) const {
	FieldInfo* fi = fieldInfo(fieldNumber);
	return (fi==NULL)?LUCENE_BLANK_STRING:fi->name;
}

FieldInfo* FieldInfos::fieldInfo(const int32_t fieldNumber) const {
	if ( fieldNumber < 0 || (size_t)fieldNumber >= byNumber.size() )
        return NULL;
    return byNumber[fieldNumber];
}

size_t FieldInfos::size()const {
	return byNumber.size();
}

bool FieldInfos::hasVectors() const{
	for (size_t i = 0; i < size(); i++) {
	   if (fieldInfo(i)->storeTermVector)
	      return true;
	}
	return false;
}

void FieldInfos::write(Directory* d, const char* name) const{
	IndexOutput* output = d->createOutput(name);
	try {
		write(output);
	} _CLFINALLY (
	    output->close();
	    _CLDELETE(output);
	);
}

void FieldInfos::write(IndexOutput* output) const{
	output->writeVInt(static_cast<int32_t>(size()));
	FieldInfo* fi;
	uint8_t bits;
	for (size_t i = 0; i < size(); ++i) {
		fi = fieldInfo(i);
		bits = 0x0;
 		if (fi->isIndexed) bits |= IS_INDEXED;
 		if (fi->storeTermVector) bits |= STORE_TERMVECTOR;
 		if (fi->storePositionWithTermVector) bits |= STORE_POSITIONS_WITH_TERMVECTOR;
 		if (fi->storeOffsetWithTermVector) bits |= STORE_OFFSET_WITH_TERMVECTOR;
 		if (fi->omitNorms) bits |= OMIT_NORMS;
		if (fi->storePayloads) bits |= STORE_PAYLOADS;
		if (fi->hasProx) bits |= TERM_FREQ_AND_POSITIONS;

		if (fi->getIndexVersion() > IndexVersion::kV0) {
			bits |= 0x80;
		}

		output->writeString(fi->name,_tcslen(fi->name));
		output->writeByte(bits);

		if (fi->getIndexVersion() > IndexVersion::kV0) {
			output->writeVInt(static_cast<int32_t>(fi->getIndexVersion()));
		}
		if (fi->getIndexVersion() >= IndexVersion::kV3) {
			output->writeVInt(fi->getFlags());
		}
	}
}

void FieldInfos::read(IndexInput* input) {
	int32_t size = input->readVInt();//read in the size
    uint8_t bits;
	bool isIndexed,storeTermVector,storePositionsWithTermVector,storeOffsetWithTermVector,omitNorms,hasProx,storePayloads;
	for (int32_t i = 0; i < size; ++i){
	    TCHAR* name = input->readString(); //we could read name into a string buffer, but we can't be sure what the maximum field length will be.
			bits = input->readByte();
   		isIndexed = (bits & IS_INDEXED) != 0;
   		storeTermVector = (bits & STORE_TERMVECTOR) != 0;
   		storePositionsWithTermVector = (bits & STORE_POSITIONS_WITH_TERMVECTOR) != 0;
   		storeOffsetWithTermVector = (bits & STORE_OFFSET_WITH_TERMVECTOR) != 0;
   		omitNorms = (bits & OMIT_NORMS) != 0;
			storePayloads = (bits & STORE_PAYLOADS) != 0;
			hasProx = (bits & TERM_FREQ_AND_POSITIONS) != 0;

   		FieldInfo* fi = addInternal(name, isIndexed, storeTermVector, storePositionsWithTermVector, storeOffsetWithTermVector, omitNorms, hasProx, storePayloads);
			if ((bits & 0x80) == 0) {
				fi->setIndexVersion(IndexVersion::kV0);
			} else {
				IndexVersion indexVersion = (IndexVersion)input->readVInt();
				fi->setIndexVersion(indexVersion);
				if (indexVersion >= IndexVersion::kV3) {
					fi->setFlags(input->readVInt());
				}
			} 
   		_CLDELETE_CARRAY(name);
	}
}

CL_NS_END

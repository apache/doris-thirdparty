/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "CLucene/_ApiHeader.h"
#include "AnalysisHeader.h"
#include "CLucene/util/StringBuffer.h"
#include "CLucene/util/_ThreadLocal.h"
#include <assert.h>

CL_NS_USE(util)
CL_NS_DEF(analysis)

Analyzer::Analyzer(){}
Analyzer::~Analyzer(){}

TokenStream* Analyzer::reusableTokenStream(const TCHAR* fieldName, CL_NS(util)::Reader* reader) {
    return tokenStream(fieldName, reader);
}

///Compares the Token for their order
class OrderCompare:LUCENE_BASE, public CL_NS(util)::Compare::_base //<Token*>
{
public:
	bool operator()( Token* t1, Token* t2 ) const{
	if(t1->startOffset()>t2->startOffset())
        return false;
    if(t1->startOffset()<t2->startOffset())
        return true;
	return true;
}
};

TokenFilter::TokenFilter(TokenStream* in, bool deleteTS):
	input(in),
	deleteTokenStream(deleteTS)
{
}
TokenFilter::~TokenFilter(){
    if ( deleteTokenStream && input!=NULL ) {input->close();_CLLDELETE( input );}
    //close(); -- ISH 04/11/09
}

// Close the input TokenStream.
void TokenFilter::close() {
    if ( input != NULL ){
		input->close();
        //if ( deleteTokenStream ) _CLDELETE( input ); -- ISH 04/11/09
    }
    //input = NULL; -- ISH 04/11/09
}

CL_NS_END

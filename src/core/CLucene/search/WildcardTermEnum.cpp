/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
* 
* Distributable under the terms of either the Apache License (Version 2.0) or 
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "CLucene/_ApiHeader.h"
#include "WildcardTermEnum.h"
#include "CLucene/index/Term.h"
#include "CLucene/index/IndexReader.h"
#include "CLucene/config/repl_wchar.h"

CL_NS_USE(index)
CL_NS_DEF(search)

    bool WildcardTermEnum::termCompare(Term* term) {
        if ( term!=NULL && __term->field() == term->field() ) {
            const TCHAR* searchText = term->text();
            const TCHAR* patternText = __term->text();
			if ( _tcsncmp( searchText, pre, preLen ) == 0 ){
               return wildcardEquals(patternText+preLen, __term->textLength()-preLen, 0, searchText, term->textLength(), preLen);
			}
        }
        _endEnum = true;
        return false;
    }

    /** Creates new WildcardTermEnum */
    WildcardTermEnum::WildcardTermEnum(IndexReader* reader, Term* term):
	    FilteredTermEnum(),
		__term(_CL_POINTER(term)),
		fieldMatch(false),
		_endEnum(false)
    {
       
		pre = stringDuplicate(term->text());

		const TCHAR* sidx = _tcschr( pre, LUCENE_WILDCARDTERMENUM_WILDCARD_STRING );
		const TCHAR* cidx = _tcschr( pre, LUCENE_WILDCARDTERMENUM_WILDCARD_CHAR );
		const TCHAR* tidx = sidx;
		if (tidx == NULL) 
			tidx = cidx;
		else if ( cidx && cidx > pre) 
			tidx = cl_min(sidx, cidx);
		CND_PRECONDITION(tidx != NULL, "tidx==NULL");
		int32_t idx = (int32_t)(tidx - pre);
		preLen = idx;
		CND_PRECONDITION(preLen<term->textLength(), "preLen >= term->textLength()");
		pre[preLen]=0; //trim end

		Term* t = _CLNEW Term(__term, pre);
		setEnum( reader->terms(t) );
		_CLDECDELETE(t);
  }

    void WildcardTermEnum::close()
    {
       if ( __term != NULL ){
         FilteredTermEnum::close();

         _CLDECDELETE(__term);
         __term = NULL;

         _CLDELETE_CARRAY( pre );
       }
    }
    WildcardTermEnum::~WildcardTermEnum() {
      close();
    }

    float_t WildcardTermEnum::difference() {
        return 1.0f;
    }

    bool WildcardTermEnum::endEnum() {
        return _endEnum;
    }
	  const char* WildcardTermEnum::getObjectName() const{ return getClassName(); }
	  const char* WildcardTermEnum::getClassName(){  return "WildcardTermEnum"; }

    bool WildcardTermEnum::wildcardEquals(const TCHAR* pattern, int32_t patternLen, int32_t patternIdx, const TCHAR* str, int32_t strLen, int32_t stringIdx)
    {
        for (int32_t p = patternIdx; ; ++p)
        {
            for (int32_t s = stringIdx; ; )
            {
                // End of str yet?
                bool sEnd = (s >= strLen);
                // End of pattern yet?
                bool pEnd = (p >= patternLen);

                // If we're looking at the end of the str...
                if (sEnd)
                {
                    // Assume the only thing left on the pattern is/are wildcards
                    bool justWildcardsLeft = true;

                    // Current wildcard position
                    int32_t wildcardSearchPos = p;
                    // While we haven't found the end of the pattern,
                	// and haven't encountered any non-wildcard characters
                    while (wildcardSearchPos < patternLen && justWildcardsLeft)
                    {
                        // Check the character at the current position
                        TCHAR wildchar = pattern[wildcardSearchPos];
                        // If it's not a wildcard character, then there is more
                  		// pattern information after this/these wildcards.

                        if (wildchar != LUCENE_WILDCARDTERMENUM_WILDCARD_CHAR && 
                        		wildchar != LUCENE_WILDCARDTERMENUM_WILDCARD_STRING){
                            justWildcardsLeft = false;
                        }else{
                        	// to prevent "cat" matches "ca??"
							if (wildchar == LUCENE_WILDCARDTERMENUM_WILDCARD_CHAR)
								return false;

                            wildcardSearchPos++; // Look at the next character
                        }
                    }

                    // This was a prefix wildcard search, and we've matched, so
                	// return true.
                    if (justWildcardsLeft)
	                	return true;
	            }
	
	            // If we've gone past the end of the str, or the pattern,
	            // return false.
	            if (sEnd || pEnd)
	                break;
	
	            // Match a single character, so continue.
				if (pattern[p] == LUCENE_WILDCARDTERMENUM_WILDCARD_CHAR)
                {
                    p += lucene_utf8charlen(pattern[p]);
                    s += lucene_utf8charlen(str[s]);
                    continue;
                }

                if (pattern[p] == LUCENE_WILDCARDTERMENUM_WILDCARD_STRING)
                {
                    // Look at the character beyond the '*'.
                    ++p;
                    // Examine the str, starting at the last character.
					for (int32_t i = strLen; i >= s; --i)
					{
						if (wildcardEquals(pattern, patternLen, p, str, strLen, i))
							return true;
					}
                    break;
                }

                if (pattern[p] != str[s])
                    break;

                // if utf-8, compare the whole
                auto len_p = lucene_utf8charlen(pattern[p]);
                auto len_s = lucene_utf8charlen(str[s]);

                if (len_p != len_s || p+len_p > patternLen || s+len_s > strLen)
                    break;
                bool is_match = true;
                for(size_t i_utf8 =1; i_utf8 < len_p; ++i_utf8)
                {
                    if (pattern[p + i_utf8] != str[s + i_utf8])
                    {
                        is_match = false;
                        break;
                    }
                }
                if(!is_match)
                    break;

                p += len_p;
                s += len_s;
        }
        return false;
      }
    }


CL_NS_END

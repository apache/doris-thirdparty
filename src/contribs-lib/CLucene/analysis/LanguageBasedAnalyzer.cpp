/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "CLucene/_ApiHeader.h"

#include "CLucene/analysis/Analyzers.h"
#include "CLucene/analysis/cjk/CJKAnalyzer.h"
#include "CLucene/analysis/jieba/ChineseTokenizer.h"
#include "CLucene/analysis/standard/StandardFilter.h"
#include "CLucene/analysis/standard/StandardTokenizer.h"
#include "CLucene/snowball/SnowballFilter.h"
#include "LanguageBasedAnalyzer.h"

CL_NS_USE(util)
CL_NS_USE2(analysis, cjk)
CL_NS_USE2(analysis, jieba)
CL_NS_USE2(analysis, standard)
CL_NS_USE2(analysis, snowball)

CL_NS_DEF(analysis)

LanguageBasedAnalyzer::LanguageBasedAnalyzer(const TCHAR *language, bool stem, AnalyzerMode mode) {
    stopSet = _CLNEW CLTCSetList;

    if (language == NULL)
        _tcsncpy(lang, LUCENE_BLANK_STRING, 100);
    else
        _tcsncpy(lang, language, 100);
    this->stem = stem;
    this->mode = mode;
    Analyzer::_lowercase = false;
}

LanguageBasedAnalyzer::~LanguageBasedAnalyzer() {
    if (streams) {
        _CLDELETE(streams->filteredTokenStream);
        _CLDELETE(streams);
    }
    _CLLDELETE(stopSet);
}

bool LanguageBasedAnalyzer::isSDocOpt() {
    if (_tcscmp(lang, _T("chinese")) == 0) {
        return true;
    }
    return false;
}

void LanguageBasedAnalyzer::setStopWords(const TCHAR** stopwords) {
    StopFilter::fillStopTable(stopSet, stopwords);
}

void LanguageBasedAnalyzer::setLanguage(const TCHAR *language) {
    _tcsncpy(lang, language, 100);
}

void LanguageBasedAnalyzer::setStem(bool s) {
    this->stem = s;
}

void LanguageBasedAnalyzer::setMode(AnalyzerMode m) {
    this->mode = m;
}

void LanguageBasedAnalyzer::initDict(const std::string &dictPath) {
    if (_tcscmp(lang, _T("chinese")) == 0) {
        CL_NS2(analysis, jieba)::ChineseTokenizer::init(dictPath);
    }
}

TokenStream *LanguageBasedAnalyzer::reusableTokenStream(const TCHAR * /*fieldName*/, CL_NS(util)::Reader *reader) {
    if (streams == nullptr) {
        streams = _CLNEW SavedStreams();
        if (_tcscmp(lang, _T("cjk")) == 0) {
            streams->tokenStream = _CLNEW CL_NS2(analysis, cjk)::CJKTokenizer(reader);
            streams->filteredTokenStream =
                    _CLNEW StopFilter(streams->tokenStream, true, stopSet);
        } else if (_tcscmp(lang, _T("chinese")) == 0) {
            streams->tokenStream = _CLNEW CL_NS2(analysis, jieba)::ChineseTokenizer(reader, mode, Analyzer::_lowercase);
            streams->filteredTokenStream = streams->tokenStream;
        } else {
            CL_NS(util)::BufferedReader* bufferedReader = reader->__asBufferedReader();

            if (bufferedReader == nullptr) {
                streams->tokenStream = _CLNEW StandardTokenizer(
                        _CLNEW CL_NS(util)::FilteredBufferedReader(reader, false), true);
            } else {
                streams->tokenStream = _CLNEW StandardTokenizer(bufferedReader);
            }

            streams->filteredTokenStream = _CLNEW StandardFilter(streams->tokenStream, true);
            if (stem) {
                streams->filteredTokenStream = _CLNEW SnowballFilter( streams->filteredTokenStream, lang, true);//todo: should check whether snowball supports the language
            }
            streams->filteredTokenStream =
                    _CLNEW LowerCaseFilter(streams->filteredTokenStream, true);
            streams->filteredTokenStream =
                    _CLNEW StopFilter(streams->filteredTokenStream, true, stopSet);
        }
    } else {
        streams->tokenStream->reset(reader);
    }

    return streams->filteredTokenStream;
}

TokenStream *LanguageBasedAnalyzer::tokenStream(const TCHAR *fieldName, Reader *reader) {
    TokenStream *ret = nullptr;
    if (_tcscmp(lang, _T("cjk")) == 0) {
        ret = _CLNEW CL_NS2(analysis, cjk)::CJKTokenizer(reader);
    } else if (_tcscmp(lang, _T("chinese")) == 0) {
        ret = _CLNEW CL_NS2(analysis, jieba)::ChineseTokenizer(reader, mode, Analyzer::_lowercase);
    } else {
        CL_NS(util)::BufferedReader* bufferedReader = reader->__asBufferedReader();

        if (bufferedReader == nullptr) {
            ret = _CLNEW StandardTokenizer(
                    _CLNEW CL_NS(util)::FilteredBufferedReader(reader, false), true);
        } else {
            ret = _CLNEW StandardTokenizer(bufferedReader);
        }

        ret = _CLNEW StandardFilter(ret, true);
        if (stem) {
            ret = _CLNEW SnowballFilter(ret, lang, true);//todo: should check whether snowball supports the language
        }
        ret = _CLNEW LowerCaseFilter(ret, true);
    }
    ret = _CLNEW StopFilter(ret, true, stopSet);

    return ret;
}

CL_NS_END

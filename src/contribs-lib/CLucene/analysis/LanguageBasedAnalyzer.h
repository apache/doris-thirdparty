/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#ifndef _lucene_analysis_languagebasedanalyzer_
#define _lucene_analysis_languagebasedanalyzer_

#include "CLucene/analysis/AnalysisHeader.h"

CL_NS_DEF(analysis)

enum class AnalyzerMode {
    Default,
    All,
    Search
};

class CLUCENE_CONTRIBS_EXPORT LanguageBasedAnalyzer : public CL_NS(analysis)::Analyzer {
    class SavedStreams : public TokenStream {
    public:
        Tokenizer* tokenStream;
        TokenStream* filteredTokenStream;

        SavedStreams():tokenStream(NULL), filteredTokenStream(NULL)
        {
        }

        void close(){}
        Token* next(Token* token) {return NULL;}
    };
    /**
     * Contains the stopwords used with the StopFilter.
     */
    CL_NS(analysis)::CLTCSetList* stopSet;
    TCHAR lang[100]{};
    bool stem;
    AnalyzerMode mode{};

public:
    explicit LanguageBasedAnalyzer(const TCHAR *language = nullptr, bool stem = true, AnalyzerMode mode = AnalyzerMode::All);
    ~LanguageBasedAnalyzer() override;

    bool isSDocOpt() override;
    void setStopWords(const TCHAR** stopwords);
    void setLanguage(const TCHAR *language);
    void setStem(bool s);
    void setMode(AnalyzerMode m);
    void initDict(const std::string &dictPath) override;
    TokenStream *tokenStream(const TCHAR *fieldName, CL_NS(util)::Reader *reader) override;
    TokenStream *reusableTokenStream(const TCHAR * /*fieldName*/, CL_NS(util)::Reader *reader) override;

private:
    SavedStreams* streams = nullptr;
};

CL_NS_END
#endif

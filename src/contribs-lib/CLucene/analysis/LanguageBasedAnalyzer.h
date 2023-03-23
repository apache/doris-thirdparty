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

class CLUCENE_CONTRIBS_EXPORT LanguageBasedAnalyzer : public CL_NS(analysis)::Analyzer {
    TCHAR lang[100]{};
    bool stem;

public:
    explicit LanguageBasedAnalyzer(const TCHAR *language = nullptr, bool stem = true);
    ~LanguageBasedAnalyzer() override;
    void setLanguage(const TCHAR *language);
    void setStem(bool s);
    void initDict(const std::string &dictPath);
    TokenStream *tokenStream(const TCHAR *fieldName, CL_NS(util)::Reader *reader) override;
    TokenStream *reusableTokenStream(const TCHAR * /*fieldName*/, CL_NS(util)::Reader *reader) override;
};

CL_NS_END
#endif

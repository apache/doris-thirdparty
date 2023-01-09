/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
* 
* Distributable under the terms of either the Apache License (Version 2.0) or 
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include <memory>
#include <fstream>

#include "CLucene/StdHeader.h"
#include "CLucene/_clucene-config.h"

#include "CLucene.h"
#include "CLucene/util/Misc.h"

using namespace lucene::analysis;

//test for memory leaks:
#ifdef _MSC_VER
#ifdef _DEBUG
	#define _CRTDBG_MAP_ALLOC
	#include <stdlib.h>
	#include <crtdbg.h>
#endif
#endif

#include <stdio.h>
#include <iostream>
#include <string.h>

using namespace std;
using namespace lucene::util;


int main( int32_t argc, char** argv ){

  std::ifstream ifs("test.txt", std::ios::binary | std::ios::in);
  if (!ifs.good()) {
    std::cerr << "Failed to open " << std::endl;
    return -1;
  }

  int64_t begin = Misc::currentTimeMillis();

  std::vector<std::wstring> lines;
  std::string line;
  while (getline(ifs, line)) {
    std::wstring wstr(line.begin(), line.end());
    lines.push_back(wstr);
  }

//  int64_t begin = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

  standard::StandardAnalyzer analyzer;

  size_t n_lines = 0;
  size_t n_words = 0;
  for (vector<wstring>::iterator iter = lines.begin(); iter != lines.end(); ++iter) {
    n_lines++;
    Reader* reader = new StringReader((*iter).c_str(), (*iter).size());
    TokenStream* tokenStream = analyzer.tokenStream(L"text", reader);
    tokenStream->reset();
    Token token;
    while (tokenStream->next(&token)) 
    {
      n_words++;
      //std::cout << token.termText() << "|";
    }
    //std::cout << std::endl;
  }

//  int64_t end = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  int64_t end = Misc::currentTimeMillis();

  std::cout << "begin = " << begin << std::endl;
  std::cout << "end = " << end << std::endl;
  std::cout << "time = " << end - begin << std::endl;
  std::cout << "n_lines = " << n_lines << " n_words = " << n_words << std::endl;
  std::cout << "---------------------------" << std::endl;

  return 0;

}

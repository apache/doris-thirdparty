/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
* 
* Distributable under the terms of either the Apache License (Version 2.0) or 
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "CLucene/StdHeader.h"
#include "CLucene/_clucene-config.h"

#include "CLucene.h"
#include "CLucene/util/Misc.h"

//test for memory leaks:
#ifdef _MSC_VER
#ifdef _DEBUG
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#include <stdlib.h>
#endif
#endif

#include <algorithm>
#include <iostream>
#include <stdio.h>
#include <string.h>

using namespace std;
using namespace lucene::util;
using namespace lucene::index;

//void DeleteFiles(const char* dir);
//void IndexFiles(const char *path, const char *target, const bool clearIndex);

void SearchFiles(const char *index, int &total);

void getStats(const char *directory);

int main(int32_t argc, char **argv) {
    //Dumper Debug
#ifdef _MSC_VER
#ifdef _DEBUG
    _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF | _CRTDBG_CHECK_ALWAYS_DF | _CRTDBG_CHECK_CRT_DF);
    _crtBreakAlloc = -1;
#endif
#endif
    uint64_t str = Misc::currentTimeMillis();
    try {
        printf("Location of files indexed: ");
        char ndx[250] = "";

        char *tmp = fgets(ndx, 250, stdin);
        if (tmp == NULL) return 1;
        ndx[strlen(ndx) - 1] = 0;

        vector<string> files;
        std::sort(files.begin(), files.end());
        Misc::listDirs(ndx, files, true);
        auto itr = files.begin();
        str = Misc::currentTimeMillis();
        int total = 0;
        while (itr != files.end()) {
            //printf("\n%s:%d\n", itr->c_str(), IndexReader::indexExists(itr->c_str()));

            if (!IndexReader::indexExists(itr->c_str())) {
                printf("\n%s:%d\n", itr->c_str(), IndexReader::indexExists(itr->c_str()));
                itr++;
                continue;
            }
            getStats(itr->c_str());
            SearchFiles(itr->c_str(), total);
            itr++;
        }
        if (total >= 0) {
            printf("term was found %d docIDs in files\n", total);
        }

    } catch (CLuceneError &err) {
        printf("Error: %s\n", err.what());
    } catch (...) {
        printf("Unknown error\n");
    }

    _lucene_shutdown();//clears all static memory

    printf("\n\nTime taken: %d ms\n\n", (int32_t) (Misc::currentTimeMillis() - str));
    return 0;
}

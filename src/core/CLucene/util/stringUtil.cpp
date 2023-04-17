//
// Created by 姜凯 on 2022/9/20.
//
#include "CLucene/_ApiHeader.h"
#include "stringUtil.h"

template <>
const char* LUCENE_BLANK_SSTRING() {
    return LUCENE_BLANK_ASTRING;
}

template <>
const TCHAR* LUCENE_BLANK_SSTRING() {
    return LUCENE_BLANK_STRING;
}

template<>
void strnCopy(char *dst, const char *src, size_t size) {
    strncpy(dst, src, size);
}
template<>
void strnCopy(TCHAR *dst, const TCHAR *src, size_t size) { _tcsncpy(dst, src, size); }

template<>
void strCopy(char *dst, const char *src) {
    strcpy(dst, src);
}
template<>
void strCopy(TCHAR *dst, const TCHAR *src) { _tcscpy(dst, src); }

template<>
int strCompare(const char *leftStr, const char *rightStr) {
    return strcmp(leftStr, rightStr);
}
template<>
int strCompare(const TCHAR *leftStr, const TCHAR *rightStr) {
    return _tcscmp(leftStr, rightStr);
}

template<>
char *strDuplicate(const char *str) {
    return strdup(str);
}
template<>
TCHAR *strDuplicate(const TCHAR *str) {
    return STRDUP_TtoT(str);
}

template<>
size_t lenOfString(const char *str) { return strlen(str); }
template<>
size_t lenOfString(const TCHAR *str) { return _tcslen(str); }
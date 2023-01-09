//
// Created by 姜凯 on 2022/9/20.
//

#ifndef _lucene_util__stringutil_H
#define _lucene_util__stringutil_H

#include "CLucene/_ApiHeader.h"

template <typename T>
const T* LUCENE_BLANK_SSTRING();

template<typename T>
void strnCopy(T *dst, const T *src, size_t size);

template<typename T>
void strCopy(T *dst, const T *src);

template<typename T>
int strCompare(const T *leftStr, const T *rightStr);

template<typename T>
T *strDuplicate(const T *str);

template<typename T>
size_t lenOfString(const T *str);
#endif//_lucene_util__stringutil_H

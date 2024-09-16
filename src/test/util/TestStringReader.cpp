/*------------------------------------------------------------------------------
* Copyright (C) 2003-2010 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/

#include "test.h"
#include "CLucene/util/CLStreams.h"
#include <stdexcept>

CL_NS_USE(util)

void testSStringReaderInit(CuTest *tc) {
  // test for char
  // test default constructor and internal status
  SStringReader<char> r11;
  CuAssertEquals(tc, 0, r11.size());
  CuAssertEquals(tc, 0, r11.position());

  char chars[5] = {'t', 'e', 's', 't', 'x'};

  // test constructor without copy and internal status
  SStringReader<char> r12 {chars, 4, false};
  CuAssertEquals(tc, 4, r12.size());
  CuAssertEquals(tc, 0, r12.position());
  CuAssertEquals(tc, 't', r12.testValueAt(0));
  CuAssertEquals(tc, 'e', r12.testValueAt(1));
  CuAssertEquals(tc, 's', r12.testValueAt(2));
  CuAssertEquals(tc, 't', r12.testValueAt(3));
  // it is 'x' as original chars since not copying 
  CuAssertEquals(tc, 'x', r12.testValueAt(4));

  // test constructor with copy and internal status
  SStringReader<char> r13 {chars, 4, true};
  CuAssertEquals(tc, 4, r13.size());
  CuAssertEquals(tc, 0, r13.position());
  CuAssertEquals(tc, 't', r13.testValueAt(0));
  CuAssertEquals(tc, 'e', r13.testValueAt(1));
  CuAssertEquals(tc, 's', r13.testValueAt(2));
  CuAssertEquals(tc, 't', r13.testValueAt(3));
  // it is 0 since only copying 4 chars and add 0
  CuAssertEquals(tc, 0, r13.testValueAt(4));


  // test for wchar
  // test default constructor and internal status
  SStringReader<wchar_t> r21;
  CuAssertEquals(tc, 0, r21.size());
  CuAssertEquals(tc, 0, r21.position());

  wchar_t wchars[5] = {'t', 'e', 's', 't', 'x'};

  // test constructor without copy and internal status
  SStringReader<wchar_t> r22 {wchars, 4, false};
  CuAssertEquals(tc, 4, r22.size());
  CuAssertEquals(tc, 0, r22.position());
  CuAssertEquals(tc, 't', r22.testValueAt(0));
  CuAssertEquals(tc, 'e', r22.testValueAt(1));
  CuAssertEquals(tc, 's', r22.testValueAt(2));
  CuAssertEquals(tc, 't', r22.testValueAt(3));
  // it is 'x' as original chars since not copying 
  CuAssertEquals(tc, 'x', r22.testValueAt(4));

  // test constructor with copy and internal status
  SStringReader<wchar_t> r23 {wchars, 4, true};
  CuAssertEquals(tc, 4, r23.size());
  CuAssertEquals(tc, 0, r23.position());
  CuAssertEquals(tc, 't', r23.testValueAt(0));
  CuAssertEquals(tc, 'e', r23.testValueAt(1));
  CuAssertEquals(tc, 's', r23.testValueAt(2));
  CuAssertEquals(tc, 't', r23.testValueAt(3));
  // it is 0 since only copying 4 chars and add 0
  CuAssertEquals(tc, 0, r23.testValueAt(4));
}

CuSuite *testStringReader(void) {
    CuSuite *suite = CuSuiteNew(_T("CLucene SStringReader Test"));

    SUITE_ADD_TEST(suite, testSStringReaderInit);

    return suite;
}

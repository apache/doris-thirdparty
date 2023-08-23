#include <chrono>
#include <random>

#include "CLucene/util/stringUtil.h"
#include "test.h"

std::string ranUtf8String(uint32_t i) {
  if (!StringUtil::is_valid_codepoint(i)) {
    return "";
  }

  std::string utf8_str;
  if (i <= 0x7F) {
    utf8_str.push_back(static_cast<char>(i));
  } else if (i <= 0x7FF) {
    utf8_str.push_back(static_cast<char>(0xC0 | (i >> 6)));
    utf8_str.push_back(static_cast<char>(0x80 | (i & 0x3F)));
  } else if (i <= 0xFFFF) {
    utf8_str.push_back(static_cast<char>(0xE0 | (i >> 12)));
    utf8_str.push_back(static_cast<char>(0x80 | ((i >> 6) & 0x3F)));
    utf8_str.push_back(static_cast<char>(0x80 | (i & 0x3F)));
  } else {
    utf8_str.push_back(static_cast<char>(0xF0 | (i >> 18)));
    utf8_str.push_back(static_cast<char>(0x80 | ((i >> 12) & 0x3F)));
    utf8_str.push_back(static_cast<char>(0x80 | ((i >> 6) & 0x3F)));
    utf8_str.push_back(static_cast<char>(0x80 | (i & 0x3F)));
  }
  return utf8_str;
}

static void testSingleUtf8(CuTest *tc) {
  for (uint32_t i = 1; i <= 0x10FFFF; ++i) {
    std::string s = ranUtf8String(i);
    if (s.empty()) continue;
    std::wstring ws = StringUtil::string_to_wstring(s);
    CLUCENE_ASSERT(ws.size() == 1);
    CLUCENE_ASSERT(ws.size() == wcslen(ws.c_str()));
  }
}

// day
unsigned getSeed() {
  auto now = std::chrono::system_clock::now();
  auto now_time_t = std::chrono::system_clock::to_time_t(now);

  std::tm now_tm = *std::localtime(&now_time_t);

  now_tm.tm_hour = 0;
  now_tm.tm_min = 0;
  now_tm.tm_sec = 0;

  auto today_time_t = std::mktime(&now_tm);
  return static_cast<unsigned>(today_time_t);
}

static void testMultiUtf8(CuTest *tc) {
  unsigned seed = getSeed();
  std::mt19937 generator(seed);

  for (int32_t i = 0; i < 10000; i++) {
    std::string s;
    int32_t k = 0;
    for (int32_t j = 0; j < 3; j++) {
      std::uniform_int_distribution<uint32_t> dis(1, 1114111);
      uint32_t random_code_point = dis(generator);
      std::string temp = ranUtf8String(random_code_point);
      if (temp.empty()) continue;
      s += temp;
      k++;
    }
    std::wstring ws = StringUtil::string_to_wstring(s);
    CLUCENE_ASSERT(ws.size() == k);
    CLUCENE_ASSERT(ws.size() == wcslen(ws.c_str()));
  }
}

string generateBlobString(int length) {
  vector<char> data(length);

  uint32_t seed = getSeed();
  std::mt19937 generator(seed);

  for (int i = 0; i < length; i++) {
    std::uniform_int_distribution<uint32_t> dis(0, 256);
    uint32_t code = dis(generator);
    data[i] = (char)code;
  }

  return string(data.begin(), data.end());
}

static void testAll(CuTest *tc) {
  for (int32_t i = 0; i < 10000; i++) {
    string s = generateBlobString(100);
    std::wstring ws = StringUtil::string_to_wstring(s);
  }
}

CuSuite *testStrConvert(void) {
  CuSuite *suite = CuSuiteNew(_T("CLucene str convert Test"));

  SUITE_ADD_TEST(suite, testSingleUtf8);
  SUITE_ADD_TEST(suite, testMultiUtf8);
  SUITE_ADD_TEST(suite, testAll);
  return suite;
}
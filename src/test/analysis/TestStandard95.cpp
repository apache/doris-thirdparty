#include <random>

#include "CLucene/_ApiHeader.h"
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#include "test.h"

void testCut(const std::string &str, std::vector<std::string> &tokens) {
  auto standard =
      std::make_unique<lucene::analysis::standard95::StandardAnalyzer>();
  auto tokenizer =
      static_cast<lucene::analysis::standard95::StandardTokenizer *>(
          standard->tokenStream(L"name", nullptr));

  lucene::util::SStringReader<char> reader;
  reader.init(str.data(), str.size(), false);

  tokenizer->reset(&reader);
  CL_NS(analysis)::Token token;
  while (tokenizer->next(&token)) {
    std::string_view term(token.termBuffer<char>(), token.termLength<char>());
    tokens.emplace_back(term);
  }

  delete tokenizer;
}

void testStandardTokenizer(CuTest *tc) {
  std::string str = "Wha\u0301t's this thing do?";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  CLUCENE_ASSERT(tokens[0] == "wha\u0301t's");
  CLUCENE_ASSERT(tokens[1] == "thing");
  CLUCENE_ASSERT(tokens[2] == "do");
}

void testStandardTokenizerMaxTokenLength(CuTest *tc) {
  std::string str = "one two three ";
  std::string longWord;
  for (int i = 0; i < 51; ++i) {
    longWord.append("abcde");
  }
  str += longWord;
  str += " four five six";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  CLUCENE_ASSERT(tokens[0] == "one");
  CLUCENE_ASSERT(tokens[1] == "two");
  CLUCENE_ASSERT(tokens[2] == "three");
  CLUCENE_ASSERT(tokens[3] == longWord);
  CLUCENE_ASSERT(tokens[4] == "four");
  CLUCENE_ASSERT(tokens[5] == "five");
  CLUCENE_ASSERT(tokens[6] == "six");
}

void testStandardTokenizerMaxTokenLength1(CuTest *tc) {
  std::string str = "one two three ";
  std::string longWord;
  for (int i = 0; i < 51; ++i) {
    longWord.append("abcde");
  }
  str += longWord;
  str += "abcde";
  str += " four five six";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  CLUCENE_ASSERT(tokens[0] == "one");
  CLUCENE_ASSERT(tokens[1] == "two");
  CLUCENE_ASSERT(tokens[2] == "three");
  CLUCENE_ASSERT(tokens[3] == longWord);
  CLUCENE_ASSERT(tokens[4] == "abcde");
  CLUCENE_ASSERT(tokens[5] == "four");
  CLUCENE_ASSERT(tokens[6] == "five");
  CLUCENE_ASSERT(tokens[7] == "six");
}

void testUtf8Str(CuTest *tc) {
  std::string str =
      "昨天我参加了一个编程比赛,遇到了一位技术很棒的朋友。His name is David, "
      "他今年28岁,电话号码是13566668888。David来自美国,他的邮箱是david@gmail."
      "com。编程比赛持续了2个小时,最后David赢得了冠军。虽然我只得了第三名,"
      "但我学到了很多编程技巧。David给我推荐了一个学习编程的网站 "
      "www.codingforeveryone.com,"
      "这个网站包含许多免费的在线课程。我们都觉得这个网站很有用,"
      "可以提高编程技能。After the contest, David and I exchanged contacts. "
      "我给David我的身份证号码1234891987654321作为联系方式。我相信通过我的努力,"
      "我的编程水平一定会越来越好。这次参加比赛让我结交了一位知音好友,"
      "也激励了我不断学习进步。";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::string old_str;
  for (auto &token : tokens) {
    old_str += token;
    old_str += "|";
  }

  std::string new_str =
      "昨|天|我|参|加|了|一|个|编|程|比|赛|遇|到|了|一|位|技|术|很|棒|的|朋|友|"
      "his|name|david|他|今|年|28|岁|电|话|号|码|是|13566668888|david|来|自|美|"
      "国|他|的|邮|箱|是|david|gmail.com|编|程|比|赛|持|续|了|2|个|小|时|最|后|"
      "david|赢|得|了|冠|军|虽|然|我|只|得|了|第|三|名|但|我|学|到|了|很|多|编|"
      "程|技|巧|david|给|我|推|荐|了|一|个|学|习|编|程|的|网|站|www."
      "codingforeveryone.com|这|个|网|站|包|含|许|多|免|费|的|在|线|课|程|我|"
      "们|都|觉|得|这|个|网|站|很|有|用|可|以|提|高|编|程|技|能|after|contest|"
      "david|i|exchanged|contacts|我|给|david|我|的|身|份|证|号|码|"
      "1234891987654321|作|为|联|系|方|式|我|相|信|通|过|我|的|努|力|我|的|编|"
      "程|水|平|一|定|会|越|来|越|好|这|次|参|加|比|赛|让|我|结|交|了|一|位|知|"
      "音|好|友|也|激|励|了|我|不|断|学|习|进|步|";

  CLUCENE_ASSERT(old_str == new_str);
}

void testArmenian(CuTest *tc) {
  std::string str =
      "Վիքիպեդիայի 13 միլիոն հոդվածները (4,600` հայերեն վիքիպեդիայում) գրվել "
      "են կամավորների կողմից ու համարյա բոլոր հոդվածները կարող է խմբագրել "
      "ցանկաց մարդ ով կարող է բացել Վիքիպեդիայի կայքը։";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::vector<std::string> new_tokens = {
      "Վիքիպեդիայի",   "13",     "միլիոն",
      "հոդվածները",    "4,600",  "հայերեն",
      "վիքիպեդիայում", "գրվել",  "են",
      "կամավորների",   "կողմից", "ու",
      "համարյա",       "բոլոր",  "հոդվածները",
      "կարող",         "է",      "խմբագրել",
      "ցանկաց",        "մարդ",   "ով",
      "կարող",         "է",      "բացել",
      "Վիքիպեդիայի",   "կայքը"};

  CLUCENE_ASSERT((tokens == new_tokens));
}

void testAmharic(CuTest *tc) {
  std::string str =
      "ዊኪፔድያ የባለ ብዙ ቋንቋ የተሟላ ትክክለኛና ነጻ መዝገበ ዕውቀት (ኢንሳይክሎፒዲያ) ነው። ማንኛውም";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::vector<std::string> new_tokens = {"ዊኪፔድያ", "የባለ",       "ብዙ", "ቋንቋ",
                                         "የተሟላ",  "ትክክለኛና",    "ነጻ", "መዝገበ",
                                         "ዕውቀት",  "ኢንሳይክሎፒዲያ", "ነው", "ማንኛውም"};

  CLUCENE_ASSERT((tokens == new_tokens));
}

void testBengali(CuTest *tc) {
  std::string str =
      "এই বিশ্বকোষ পরিচালনা করে উইকিমিডিয়া ফাউন্ডেশন (একটি অলাভজনক সংস্থা)। "
      "উইকিপিডিয়ার শুরু ১৫ জানুয়ারি, ২০০১ সালে। এখন পর্যন্ত ২০০টিরও বেশী ভাষায় "
      "উইকিপিডিয়া রয়েছে।";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::vector<std::string> new_tokens = {
      "এই",         "বিশ্বকোষ", "পরিচালনা", "করে",   "উইকিমিডিয়া",
      "ফাউন্ডেশন",   "একটি",    "অলাভজনক",  "সংস্থা", "উইকিপিডিয়ার",
      "শুরু",         "১৫",      "জানুয়ারি",  "২০০১",  "সালে",
      "এখন",        "পর্যন্ত",   "২০০টিরও",  "বেশী",  "ভাষায়",
      "উইকিপিডিয়া", "রয়েছে"};

  CLUCENE_ASSERT((tokens == new_tokens));
}

void testFarsi(CuTest *tc) {
  std::string str =
      "ویکی پدیای انگلیسی در تاریخ ۲۵ دی ۱۳۷۹ به صورت مکملی برای دانشنامهٔ "
      "تخصصی نوپدیا نوشته شد.";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::vector<std::string> new_tokens = {
      "ویکی",     "پدیای", "انگلیسی", "در",    "تاریخ", "۲۵",
      "دی",       "۱۳۷۹",  "به",      "صورت",  "مکملی", "برای",
      "دانشنامهٔ", "تخصصی", "نوپدیا",  "نوشته", "شد"};

  CLUCENE_ASSERT((tokens == new_tokens));
}

void testGreek(CuTest *tc) {
  std::string str =
      "Γράφεται σε συνεργασία από εθελοντές με το λογισμικό wiki, κάτι που "
      "σημαίνει ότι άρθρα μπορεί να προστεθούν ή να αλλάξουν από τον καθένα.";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::vector<std::string> new_tokens = {
      "Γράφεται", "σε",        "συνεργασία", "από",  "εθελοντές",  "με",
      "το",       "λογισμικό", "wiki",       "κάτι", "που",        "σημαίνει",
      "ότι",      "άρθρα",     "μπορεί",     "να",   "προστεθούν", "ή",
      "να",       "αλλάξουν",  "από",        "τον",  "καθένα"};

  CLUCENE_ASSERT((tokens == new_tokens));
}

void testThai(CuTest *tc) {
  std::string str = "การที่ได้ต้องแสดงว่างานดี. แล้วเธอจะไปไหน? ๑๒๓๔";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::vector<std::string> new_tokens = {"การที่ได้ต้องแสดงว่างานดี", "แล้วเธอจะไปไหน",
                                         "๑๒๓๔"};

  CLUCENE_ASSERT((tokens == new_tokens));
}

void testLao(CuTest *tc) {
  std::string str = "ສາທາລະນະລັດ ປະຊາທິປະໄຕ ປະຊາຊົນລາວ";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::vector<std::string> new_tokens = {"ສາທາລະນະລັດ", "ປະຊາທິປະໄຕ",
                                         "ປະຊາຊົນລາວ"};

  CLUCENE_ASSERT((tokens == new_tokens));
}

void testTibetan(CuTest *tc) {
  std::string str =
      "སྣོན་མཛོད་དང་ལས་འདིས་བོད་ཡིག་མི་ཉམས་གོང་འཕེལ་དུ་གཏོང་བར་ཧ་ཅང་དགེ་མཚན་མཆིས་སོ། །";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::vector<std::string> new_tokens = {
      "སྣོན",  "མཛོད", "དང",  "ལས", "འདིས", "བོད", "ཡིག", "མི",   "ཉམས", "གོང",
      "འཕེལ", "དུ",   "གཏོང", "བར", "ཧ",   "ཅང", "དགེ", "མཚན", "མཆིས", "སོ"};

  CLUCENE_ASSERT((tokens == new_tokens));
}

static void testChinese(CuTest *tc) {
  std::string str = "我是中国人。 １２３４ Ｔｅｓｔｓ ";
  std::vector<std::string> tokens;
  testCut(str, tokens);

  std::vector<std::string> new_tokens = {"我", "是",       "中",        "国",
                                         "人", "１２３４", "Ｔｅｓｔｓ"};

  CLUCENE_ASSERT((tokens == new_tokens));
}

CuSuite *teststandard95(void) {
  CuSuite *suite = CuSuiteNew(_T("CLucene Standard95 Test"));

  SUITE_ADD_TEST(suite, testStandardTokenizer);
  SUITE_ADD_TEST(suite, testStandardTokenizerMaxTokenLength);
  SUITE_ADD_TEST(suite, testStandardTokenizerMaxTokenLength1);
  SUITE_ADD_TEST(suite, testUtf8Str);
  SUITE_ADD_TEST(suite, testArmenian);
  SUITE_ADD_TEST(suite, testAmharic);
  SUITE_ADD_TEST(suite, testBengali);
  SUITE_ADD_TEST(suite, testFarsi);
  SUITE_ADD_TEST(suite, testGreek);
  SUITE_ADD_TEST(suite, testThai);
  SUITE_ADD_TEST(suite, testLao);
  SUITE_ADD_TEST(suite, testTibetan);
  SUITE_ADD_TEST(suite, testChinese);
  return suite;
}
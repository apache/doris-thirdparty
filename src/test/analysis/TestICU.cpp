#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "CLucene/analysis/icu/ICUAnalyzer.h"
#include "CLucene/analysis/icu/ICUTokenizer.h"
#include "CLucene/debug/error.h"
#include "test.h"

using namespace lucene::analysis;

void tokenize(const std::string& s, std::vector<std::string>& datas) {
    try {
        ICUAnalyzer analyzer;
        analyzer.initDict("./icu-dict");
        analyzer.set_lowercase(false);

        lucene::util::SStringReader<char> reader;
        reader.init(s.data(), s.size(), false);

        std::unique_ptr<ICUTokenizer> tokenizer;
        tokenizer.reset((ICUTokenizer*)analyzer.tokenStream(L"", &reader));

        Token t;
        while (tokenizer->next(&t)) {
            std::string term(t.termBuffer<char>(), t.termLength<char>());
            datas.emplace_back(term);
        }
    } catch (CLuceneError& e) {
        std::cout << "error: " << e.what() << ", " << s << std::endl;
        assert(false);
    }
}

void testICUTokenizer(CuTest* tc) {
    std::vector<std::string> datas;

    // Chinese text
    std::string chineseText =
            "今天天气真好，我们一起去公园散步吧。人工智能正在改变我们的生活方式。这本书的内容非常有"
            "趣，我推荐给你。";
    tokenize(chineseText, datas);
    assert(datas.size() == 27);
    datas.clear();

    // English text
    std::string englishText =
            "The quick brown fox jumps over the lazy dog. Artificial intelligence is transforming "
            "various industries. Reading books can significantly enhance your knowledge.";
    tokenize(englishText, datas);
    assert(datas.size() == 22);
    datas.clear();

    // Vietnamese text
    std::string vietnameseText =
            "Hôm nay thời tiết thật đẹp, chúng ta cùng đi dạo công viên nhé. Trí tuệ nhân tạo đang "
            "thay đổi cách sống của chúng ta. Cuốn sách này rất thú vị, tôi muốn giới thiệu cho "
            "bạn.";
    tokenize(vietnameseText, datas);
    assert(datas.size() == 38);
    datas.clear();

    // Portuguese text
    std::string portugueseText =
            "O tempo está ótimo hoje, vamos dar um passeio no parque. A inteligência artificial "
            "está transformando nossas vidas. Este livro é muito interessante, eu recomendo para "
            "você.";
    tokenize(portugueseText, datas);
    assert(datas.size() == 27);
    datas.clear();

    // Indonesian text
    std::string indonesianText =
            "Hari ini cuaca sangat bagus, mari kita jalan-jalan ke taman. Kecerdasan buatan sedang "
            "mengubah cara hidup kita. Buku ini sangat menarik, mari kita rekomendasikan.";
    tokenize(indonesianText, datas);
    assert(datas.size() == 25);
    datas.clear();

    // Spanish text
    std::string spanishText =
            "Hoy hace muy buen tiempo, vamos a pasear por el parque. La inteligencia artificial "
            "está cambiando nuestras vidas. Este libro es muy interesante, te lo recomiendo.";
    tokenize(spanishText, datas);
    assert(datas.size() == 26);
    datas.clear();

    // Thai text
    std::string thaiText =
            "วันนี้อากาศดีมาก "
            "เราไปเดินเล่นที่สวนสาธารณะกันเถอะปัญญาประดิษฐ์กำลังเปลี่ยนวิถีชีวิตของเราหนังสือเล่มนี้น่าสนใจมาก "
            "ฉันอยากแนะนำให้คุณอ่าน";
    tokenize(thaiText, datas);
    assert(datas.size() == 34);
    datas.clear();

    // Hindi text
    std::string hindiText =
            "आज मौसम बहुत अच्छा है, चलो पार्क में टहलने चलते हैं। कृत्रिम बुद्धिमत्ता हमारे जीवन को बदल रही है। यह "
            "किताब बहुत दिलचस्प है, मैं इसे आपको सुझाता हूं।";
    tokenize(hindiText, datas);
    assert(datas.size() == 29);
    datas.clear();
}

void testICUTokenizerEmptyText(CuTest* tc) {
    std::vector<std::string> datas;
    std::string emptyText = "";
    tokenize(emptyText, datas);
    assert(datas.size() == 0);
}

void testICUTokenizerSingleWord(CuTest* tc) {
    std::vector<std::string> datas;

    // Chinese word
    std::string chineseText = "天气";
    tokenize(chineseText, datas);
    assert(datas.size() == 1);
    datas.clear();

    // English word
    std::string englishText = "weather";
    tokenize(englishText, datas);
    assert(datas.size() == 1);
    datas.clear();

    // Arabic word
    std::string arabicText = "الذكاء";
    tokenize(arabicText, datas);
    assert(datas.size() == 1);
}

void testICUTokenizerMultipleSpaces(CuTest* tc) {
    std::vector<std::string> datas;
    std::string multipleSpacesText = "The    quick    brown   fox";
    tokenize(multipleSpacesText, datas);
    assert(datas.size() == 4);
}

void testICUTokenizerPunctuation(CuTest* tc) {
    std::vector<std::string> datas;
    std::string textWithPunctuation = "Hello, world! How's it going?";
    tokenize(textWithPunctuation, datas);
    assert(datas.size() == 5);
}

void testICUTokenizerMixedLanguage(CuTest* tc) {
    std::vector<std::string> datas;
    std::string mixedText = "Hello, 今天天气真好!";
    tokenize(mixedText, datas);
    assert(datas.size() == 4);
}

void testICUTokenizerUnicode(CuTest* tc) {
    std::vector<std::string> datas;
    std::string unicodeText = "你好，世界! 😊🌍";
    tokenize(unicodeText, datas);
    assert(datas.size() == 4);
}

void testICUTokenizerNumericText(CuTest* tc) {
    std::vector<std::string> datas;
    std::string numericText = "The price is 100 dollars.";
    tokenize(numericText, datas);
    assert(datas.size() == 5);
}

void testICUTokenizerLongText(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longText =
            "Artificial intelligence is rapidly changing various industries around the world. "
            "From healthcare to finance, it is transforming the way we work, live, and interact "
            "with technology.";
    tokenize(longText, datas);
    assert(datas.size() == 26);
}

void testICUTokenizerSpecialCharacters(CuTest* tc) {
    std::vector<std::string> datas;
    std::string specialCharsText = "@#$%^&*()_+{}[]|:;\"'<>,.?/~`";
    tokenize(specialCharsText, datas);
    assert(datas.size() == 0);
}

void testICUTokenizerLongWords(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "hippopotomonstrosesquipedaliophobia";
    tokenize(longWordText, datas);
    assert(datas.size() == 1);
}

void testICUArmenian(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText =
            "Վիքիպեդիայի 13 միլիոն հոդվածները (4,600` հայերեն վիքիպեդիայում) գրվել են կամավորների "
            "կողմից ու համարյա բոլոր հոդվածները կարող է խմբագրել ցանկաց մարդ ով կարող է բացել "
            "Վիքիպեդիայի կայքը։";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {
            "Վիքիպեդիայի",   "13",    "միլիոն",     "հոդվածները",  "4,600",  "հայերեն",
            "վիքիպեդիայում", "գրվել", "են",         "կամավորների", "կողմից", "ու",
            "համարյա",       "բոլոր", "հոդվածները", "կարող",       "է",      "խմբագրել",
            "ցանկաց",        "մարդ",  "ով",         "կարող",       "է",      "բացել",
            "Վիքիպեդիայի",   "կայքը"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUAmharic(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "ዊኪፔድያ የባለ ብዙ ቋንቋ የተሟላ ትክክለኛና ነጻ መዝገበ ዕውቀት (ኢንሳይክሎፒዲያ) ነው። ማንኛውም";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"ዊኪፔድያ", "የባለ",  "ብዙ",   "ቋንቋ",       "የተሟላ", "ትክክለኛና",
                                       "ነጻ",    "መዝገበ", "ዕውቀት", "ኢንሳይክሎፒዲያ", "ነው",   "ማንኛውም"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUArabic(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText =
            "الفيلم الوثائقي الأول عن ويكيبيديا يسمى \"الحقيقة بالأرقام: قصة ويكيبيديا\" "
            "(بالإنجليزية: Truth in Numbers: The Wikipedia Story)، سيتم إطلاقه في 2008.";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {
            "الفيلم",   "الوثائقي",  "الأول",     "عن",          "ويكيبيديا", "يسمى", "الحقيقة",
            "بالأرقام", "قصة",       "ويكيبيديا", "بالإنجليزية", "Truth",     "in",   "Numbers",
            "The",      "Wikipedia", "Story",     "سيتم",        "إطلاقه",    "في",   "2008"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUAramaic(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText =
            "ܘܝܩܝܦܕܝܐ (ܐܢܓܠܝܐ: Wikipedia) ܗܘ ܐܝܢܣܩܠܘܦܕܝܐ ܚܐܪܬܐ ܕܐܢܛܪܢܛ ܒܠܫܢ̈ܐ ܣܓܝܐ̈ܐ܂ ܫܡܗ ܐܬܐ ܡܢ "
            "ܡ̈ܠܬܐ ܕ\"ܘܝܩܝ\" ܘ\"ܐܝܢܣܩܠܘܦܕܝܐ\"܀";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {
            "ܘܝܩܝܦܕܝܐ", "ܐܢܓܠܝܐ", "Wikipedia", "ܗܘ",  "ܐܝܢܣܩܠܘܦܕܝܐ", "ܚܐܪܬܐ",
            "ܕܐܢܛܪܢܛ",  "ܒܠܫܢ̈ܐ",  "ܣܓܝܐ̈ܐ",     "ܫܡܗ", "ܐܬܐ",         "ܡܢ",
            "ܡ̈ܠܬܐ",     "ܕ",      "ܘܝܩܝ",      "ܘ",   "ܐܝܢܣܩܠܘܦܕܝܐ"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUBengali(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText =
            "এই বিশ্বকোষ পরিচালনা করে উইকিমিডিয়া ফাউন্ডেশন (একটি অলাভজনক সংস্থা)। উইকিপিডিয়ার শুরু ১৫ "
            "জানুয়ারি, ২০০১ সালে। এখন পর্যন্ত ২০০টিরও বেশী ভাষায় উইকিপিডিয়া রয়েছে।܀";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"এই",         "বিশ্বকোষ", "পরিচালনা", "করে",   "উইকিমিডিয়া",
                                       "ফাউন্ডেশন",   "একটি",    "অলাভজনক",  "সংস্থা", "উইকিপিডিয়ার",
                                       "শুরু",         "১৫",      "জানুয়ারি",  "২০০১",  "সালে",
                                       "এখন",        "পর্যন্ত",   "২০০টিরও",  "বেশী",  "ভাষায়",
                                       "উইকিপিডিয়া", "রয়েছে"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUFarsi(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText =
            "ویکی پدیای انگلیسی در تاریخ ۲۵ دی ۱۳۷۹ به صورت مکملی برای دانشنامهٔ تخصصی نوپدیا نوشته "
            "شد.";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"ویکی",     "پدیای", "انگلیسی", "در",    "تاریخ", "۲۵",
                                       "دی",       "۱۳۷۹",  "به",      "صورت",  "مکملی", "برای",
                                       "دانشنامهٔ", "تخصصی", "نوپدیا",  "نوشته", "شد"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUGreek(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText =
            "Γράφεται σε συνεργασία από εθελοντές με το λογισμικό wiki, κάτι που σημαίνει ότι "
            "άρθρα μπορεί να προστεθούν ή να αλλάξουν από τον καθένα.";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"Γράφεται", "σε",         "συνεργασία", "από",   "εθελοντές",
                                       "με",       "το",         "λογισμικό",  "wiki",  "κάτι",
                                       "που",      "σημαίνει",   "ότι",        "άρθρα", "μπορεί",
                                       "να",       "προστεθούν", "ή",          "να",    "αλλάξουν",
                                       "από",      "τον",        "καθένα"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUKhmer(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "ផ្ទះស្កឹមស្កៃបីបួនខ្នងនេះ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"ផ្ទះ", "ស្កឹមស្កៃ", "បី", "បួន", "ខ្នង", "នេះ"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICULao(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "ກວ່າດອກ ພາສາລາວ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"ກວ່າ", "ດອກ", "ພາສາ", "ລາວ"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUMyanmar(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "သက်ဝင်လှုပ်ရှားစေပြီး";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"သက်ဝင်", "လှုပ်ရှား", "စေ", "ပြီး"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUThai(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "การที่ได้ต้องแสดงว่างานดี. แล้วเธอจะไปไหน? ๑๒๓๔";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"การ", "ที่",   "ได้",  "ต้อง", "แสดง", "ว่า",  "งาน",
                                       "ดี",   "แล้ว", "เธอ", "จะ",  "ไป",   "ไหน", "๑๒๓๔"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUTibetan(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "སྣོན་མཛོད་དང་ལས་འདིས་བོད་ཡིག་མི་ཉམས་གོང་འཕེལ་དུ་གཏོང་བར་ཧ་ཅང་དགེ་མཚན་མཆིས་སོ། །";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"སྣོན", "མཛོད", "དང", "ལས",  "འདིས", "བོད",  "ཡིག",
                                       "མི",  "ཉམས", "གོང", "འཕེལ", "དུ",   "གཏོང", "བར",
                                       "ཧ",  "ཅང",  "དགེ", "མཚན", "མཆིས", "སོ"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUChinese(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "我是中国人。 １２３４ Ｔｅｓｔｓ ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"我是", "中国人", "１２３４", "Ｔｅｓｔｓ"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUHebrew(CuTest* tc) {
    {
        std::vector<std::string> datas;
        std::string longWordText = "דנקנר תקף את הדו\"ח";
        tokenize(longWordText, datas);
        std::vector<std::string> result = {"דנקנר", "תקף", "את", "הדו\"ח"};
        for (size_t i = 0; i < datas.size(); i++) {
            assert(datas[i] == result[i]);
        }
    }
    {
        std::vector<std::string> datas;
        std::string longWordText = "חברת בת של מודי'ס";
        tokenize(longWordText, datas);
        std::vector<std::string> result = {"חברת", "בת", "של", "מודי'ס"};
        for (size_t i = 0; i < datas.size(); i++) {
            assert(datas[i] == result[i]);
        }
    }
}

void testICUEmpty(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = " . ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICULUCENE1545(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "moͤchte";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"moͤchte"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUAlphanumericSA(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "B2B 2B";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"B2B", "2B"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUDelimitersSA(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "some-dashed-phrase dogs,chase,cats ac/dc";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"some",  "dashed", "phrase", "dogs",
                                       "chase", "cats",   "ac",     "dc"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUApostrophesSA(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "O'Reilly you're she's Jim's don't O'Reilly's";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"O'Reilly", "you're", "she's",
                                       "Jim's",    "don't",  "O'Reilly's"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUNumericSA(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "21.35 R2D2 C3PO 216.239.63.104 216.239.63.104";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"21.35", "R2D2", "C3PO", "216.239.63.104", "216.239.63.104"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUTextWithNumbersSA(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "David has 5000 bones";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"David", "has", "5000", "bones"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUVariousTextSA(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText =
            "C embedded developers wanted foo bar FOO BAR foo      bar .  FOO <> BAR \"QUOTED\" "
            "word";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"C",   "embedded", "developers", "wanted", "foo",
                                       "bar", "FOO",      "BAR",        "foo",    "bar",
                                       "FOO", "BAR",      "QUOTED",     "word"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUKoreanSA(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "안녕하세요 한글입니다";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"안녕하세요", "한글입니다"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUReusableTokenStream(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "སྣོན་མཛོད་དང་ལས་འདིས་བོད་ཡིག་མི་ཉམས་གོང་འཕེལ་དུ་གཏོང་བར་ཧ་ཅང་དགེ་མཚན་མཆིས་སོ། །";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"སྣོན", "མཛོད", "དང", "ལས",  "འདིས", "བོད",  "ཡིག",
                                       "མི",  "ཉམས", "གོང", "འཕེལ", "དུ",   "གཏོང", "བར",
                                       "ཧ",  "ཅང",  "དགེ", "མཚན", "མཆིས", "སོ"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUOffsets(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "David has 5000 bones";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"David", "has", "5000", "bones"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUKorean(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "훈민정음";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"훈민정음"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUJapanese(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "仮名遣い カタカナ";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"仮名遣い", "カタカナ"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUEmoji(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText =
            "💩 💩💩 👩‍❤️‍👩 👨🏼‍⚕️ 🇺🇸🇺🇸 #️⃣ 3️⃣ "
            "🏴󠁧󠁢󠁥󠁮󠁧󠁿";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {
            "💩", "💩", "💩", "👩‍❤️‍👩",        "👨🏼‍⚕️", "🇺🇸",
            "🇺🇸", "#️⃣",  "3️⃣",  "🏴󠁧󠁢󠁥󠁮󠁧󠁿"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUEmojiTokenization(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "poo💩poo 💩中國💩";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"poo", "💩", "poo", "💩", "中國", "💩"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

void testICUScriptExtensions(CuTest* tc) {
    std::vector<std::string> datas;
    std::string longWordText = "𑅗० 𑅗ा 𑅗᪾";
    tokenize(longWordText, datas);
    std::vector<std::string> result = {"𑅗०", "𑅗ा", "𑅗᪾"};
    for (size_t i = 0; i < datas.size(); i++) {
        assert(datas[i] == result[i]);
    }
}

CuSuite* testICU(void) {
    CuSuite* suite = CuSuiteNew(_T("CLucene ICU Test"));

    SUITE_ADD_TEST(suite, testICUTokenizer);
    SUITE_ADD_TEST(suite, testICUTokenizerEmptyText);
    SUITE_ADD_TEST(suite, testICUTokenizerSingleWord);
    SUITE_ADD_TEST(suite, testICUTokenizerSingleWord);
    SUITE_ADD_TEST(suite, testICUTokenizerMultipleSpaces);
    SUITE_ADD_TEST(suite, testICUTokenizerPunctuation);
    SUITE_ADD_TEST(suite, testICUTokenizerMixedLanguage);
    SUITE_ADD_TEST(suite, testICUTokenizerUnicode);
    SUITE_ADD_TEST(suite, testICUTokenizerNumericText);
    SUITE_ADD_TEST(suite, testICUTokenizerLongText);
    SUITE_ADD_TEST(suite, testICUTokenizerSpecialCharacters);
    SUITE_ADD_TEST(suite, testICUTokenizerLongWords);
    SUITE_ADD_TEST(suite, testICUArmenian);
    SUITE_ADD_TEST(suite, testICUAmharic);
    SUITE_ADD_TEST(suite, testICUArabic);
    SUITE_ADD_TEST(suite, testICUAramaic);
    SUITE_ADD_TEST(suite, testICUBengali);
    SUITE_ADD_TEST(suite, testICUFarsi);
    SUITE_ADD_TEST(suite, testICUGreek);
    SUITE_ADD_TEST(suite, testICUKhmer);
    SUITE_ADD_TEST(suite, testICULao);
    SUITE_ADD_TEST(suite, testICUMyanmar);
    SUITE_ADD_TEST(suite, testICUThai);
    SUITE_ADD_TEST(suite, testICUTibetan);
    SUITE_ADD_TEST(suite, testICUChinese);
    SUITE_ADD_TEST(suite, testICUHebrew);
    SUITE_ADD_TEST(suite, testICUEmpty);
    SUITE_ADD_TEST(suite, testICULUCENE1545);
    SUITE_ADD_TEST(suite, testICUAlphanumericSA);
    SUITE_ADD_TEST(suite, testICUDelimitersSA);
    SUITE_ADD_TEST(suite, testICUApostrophesSA);
    SUITE_ADD_TEST(suite, testICUNumericSA);
    SUITE_ADD_TEST(suite, testICUTextWithNumbersSA);
    SUITE_ADD_TEST(suite, testICUVariousTextSA);
    SUITE_ADD_TEST(suite, testICUKoreanSA);
    SUITE_ADD_TEST(suite, testICUReusableTokenStream);
    SUITE_ADD_TEST(suite, testICUOffsets);
    SUITE_ADD_TEST(suite, testICUKorean);
    SUITE_ADD_TEST(suite, testICUJapanese);
    SUITE_ADD_TEST(suite, testICUEmoji);
    SUITE_ADD_TEST(suite, testICUEmojiTokenization);
    SUITE_ADD_TEST(suite, testICUScriptExtensions);

    return suite;
}
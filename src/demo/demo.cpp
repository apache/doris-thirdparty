#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <codecvt>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <locale>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "CLucene/search/TermQuery.h"
#include "CLucene/store/Directory.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/util/NumericUtils.h"
#include "CLucene/util/bkd/bkd_writer.h"
#include "simdjson.h"

CL_NS_USE(util)
CL_NS_USE(store)
CL_NS_USE(search)
CL_NS_USE(index)

using namespace simdjson;
using namespace std::chrono;

class TimeGuard {
public:
    TimeGuard(std::string message) : message_(std::move(message)) {
        begin_ = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    ~TimeGuard() {
        int64_t end = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        std::cout << message_ << ": " << end - begin_ << std::endl;
    }

private:
    std::string message_;
    int64_t begin_ = 0;
};

inline std::wstring to_wide_string(const std::string& input) {
    std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
    return converter.from_bytes(input);
}

void get_process_physical_hold(int64_t& phy_hold) {
    phy_hold = 0;
    int64_t unused = 0;
    char buffer[1024] = "";
    FILE* file = fopen("/proc/self/status", "r");
    if (NULL != file) {
        while (fscanf(file, " %1023s", buffer) == 1) {
            if (strcmp(buffer, "VmRSS:") == 0) {
                fscanf(file, " %ld", &phy_hold);
            }
            if (strcmp(buffer, "VmHWM:") == 0) {
                fscanf(file, " %ld", &unused);
            }
            if (strcmp(buffer, "VmSize:") == 0) {
                fscanf(file, " %ld", &unused);
            }
            if (strcmp(buffer, "VmPeak:") == 0) {
                fscanf(file, " %ld", &unused);
            }
        }
        fclose(file);
    }
}

int main() {
    srand((unsigned)time(NULL));

    // std::locale loc("zh_CN.UTF-8");
    // std::locale::global(loc);

    // std::thread t([]() {
    //     for (;;) {
    //         std::this_thread::sleep_for(std::chrono::seconds(2));
    //         int64_t process_hold = 0;
    //         get_process_physical_hold(process_hold);
    //         std::cout << "physicalMem: " << process_hold / 1024 << std::endl;
    //     }
    // });
    // t.detach();

    std::string name = "description";
    std::unordered_map<std::string, std::vector<std::string>> datas;
    {
        for (int32_t i = 1; i <= 6; i++) {
            std::ifstream ifs;
            std::string path = "/mnt/disk2/yangsiyu/git_events/2022-09-13-";
            path += std::to_string(i + 5);
            path += ".json";
            ifs.open(path, std::ios::in);
            std::string line;
            ondemand::parser parser;
            int j = 0;
            while (getline(ifs, line)) {
                // padded_string json(line);
                // ondemand::document tweet = parser.iterate(json);
                // for (auto d : tweet.get_object()) {
                //     std::string key = (std::string)d.unescaped_key().value();
                //     if (key == "payload") {
                //         for (auto d1 : d.value().get_object()) {
                //             std::string key1 = (std::string)d1.unescaped_key().value();
                //             if (key1 == name) {
                //                 if (!d1.value().is_null()) {
                //                     std::string value =
                //                             (std::string)d1.value().raw_json_token().value();
                //                     // if (value == "\"https://api.github.com/users/Juveniel\"") {
                //                     //     static int32_t a = 0;
                //                     //     std::cout << key1 << ", " << value << ", " << a++ << std::endl;
                //                     // }
                //                     datas[key1].emplace_back(std::move(value));
                //                 }
                //             }
                //         }
                //     }

                //     // if (key == name) {
                //     //     std::string value = (std::string)d.value().raw_json_token().value();
                //     //     datas[key].emplace_back(std::move(value));
                //     // }
                // }
                datas[name].emplace_back(line);
                // if (++j == 10) {
                //     break;
                // }
            }
            ifs.close();
        }
    }

    // {
    //     std::ofstream ofs;
    //     ofs.open("/mnt/disk2/yangsiyu/git_events/a.txt");
    //     for (auto& data : datas[name]) {
    //         ofs << data << std::endl;
    //     }
    //     ofs.close();
    // }

    // {
    //     std::ifstream ifs;
    //     ifs.open("/mnt/disk2/yangsiyu/git_events/a.txt");
    //     std::string line;
    //     while (getline(ifs, line)) {
    //         datas[name].push_back(line);
    //     }
    //     ifs.close();
    // }

    std::cout << "getchar: " << datas[name].size() << ", pid: " << getpid() << std::endl;
    getchar();

    // std::vector<std::thread> threads;
    // for (int32_t i = 0; i < 3; i++) {
    //     threads.emplace_back([&datas, i]() {
    for (int32_t k = 0; k < 1; k++) {
        TimeGuard t("time");

        std::string path = "/mnt/disk2/yangsiyu/clucene/index" + std::to_string(k + 1);
        // auto analyzer = _CLNEW lucene::analysis::standard::StandardAnalyzer();
        auto analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<char>();
        // auto analyzer = _CLNEW lucene::analysis::SimpleAnalyzer<TCHAR>();
        auto indexwriter = _CLNEW lucene::index::IndexWriter(path.c_str(), analyzer, true);
        indexwriter->setRAMBufferSizeMB(2048);
        indexwriter->setMaxFieldLength(0x7FFFFFFFL);
        indexwriter->setMergeFactor(100000000);
        indexwriter->setUseCompoundFile(false);

        auto char_string_reader = _CLNEW lucene::util::SStringReader<char>;

        auto doc = _CLNEW lucene::document::Document();
        auto field_config =
                lucene::document::Field::STORE_NO | lucene::document::Field::INDEX_NONORMS;
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_name = std::wstring(name.begin(), name.end());
        auto field = _CLNEW lucene::document::Field(field_name.c_str(), field_config);
        field->setOmitTermFreqAndPositions(false);
        doc->add(*field);

        for (int32_t j = 0; j < 1; j++) {
            for (auto& str : datas[name]) {
                // std::cout << "name: " << name << ", value: " << str << std::endl;
                // auto field_value = to_wide_string(str);
                // field->setValue(field_value.data(), field_value.size());

                char_string_reader->init(str.data(), str.size(), false);
                auto stream = analyzer->reusableTokenStream(field->name(), char_string_reader);
                field->setValue(stream);

                indexwriter->addDocument(doc);
            }
        }

        std::cout << "---------------------" << std::endl;

        indexwriter->close();

        _CLLDELETE(indexwriter);
        _CLLDELETE(doc);
        _CLLDELETE(analyzer);
        _CLLDELETE(char_string_reader);
    }
    //     });
    // }

    // for (int32_t i = 0; i < 3; i++) {
    //     threads[i].join();
    // }

    std::cout << "---------------------" << std::endl;

    auto indexCompaction = [&datas, &name](std::vector<lucene::store::Directory*> srcDirs,
                                           std::vector<lucene::store::Directory*> destDirs,
                                           int32_t count) {
        auto indexwriter = _CLNEW lucene::index::IndexWriter("/mnt/disk2/yangsiyu/clucene/index0",
                                                             nullptr, true);

        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec(
                srcDirs.size(), std::vector<std::pair<uint32_t, uint32_t>>(count));
        int32_t idx = 0;
        int32_t id = 0;
        for (int32_t i = 0; i < count; i++) {
            for (int32_t j = 0; j < srcDirs.size(); j++) {
                if (id == count * destDirs.size()) {
                    idx++;
                    id = 0;
                }
                trans_vec[j][i] = std::make_pair(idx, id++);
            }
        }

        std::vector<uint32_t> dest_index_docs(destDirs.size());
        for (int32_t i = 0; i < destDirs.size(); i++) {
            dest_index_docs[i] = count * destDirs.size();
        }

        indexwriter->indexCompaction(srcDirs, destDirs, std::make_shared<const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>>(trans_vec), dest_index_docs);
        indexwriter->close();
        _CLDELETE(indexwriter);
    };

    // index compaction
    {
        {
            std::vector<lucene::store::Directory*> srcDirs;
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index1"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index1"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index1"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index1"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index1"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index1"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index1"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index1"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index1"));
            std::vector<lucene::store::Directory*> destDirs;
            destDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index10"));
            destDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index11"));
            destDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index12"));
            indexCompaction(srcDirs, destDirs, datas[name].size());
            for (auto& p : srcDirs) {
                p->close();
                _CLDECDELETE(p);
            }
            for (auto& p : destDirs) {
                p->close();
                _CLDECDELETE(p);
            }
        }
        {
            std::vector<lucene::store::Directory*> srcDirs;
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index10"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index11"));
            srcDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index12"));
            std::vector<lucene::store::Directory*> destDirs;
            destDirs.push_back(FSDirectory::getDirectory("/mnt/disk2/yangsiyu/clucene/index13"));
            indexCompaction(srcDirs, destDirs, datas[name].size() * 3 * 3);
            for (auto& p : srcDirs) {
                p->close();
                _CLDECDELETE(p);
            }
            for (auto& p : destDirs) {
                p->close();
                _CLDECDELETE(p);
            }
        }
    }

    // search
    {
            std::vector<int32_t> v;
            v.push_back(1);
            v.push_back(10);
            v.push_back(11);
            v.push_back(12);
            v.push_back(13);
            for (auto idx : v) {
                std::cout << "---------------------" << std::endl;
                std::string path = "/mnt/disk2/yangsiyu/clucene/index" + std::to_string(idx);
                IndexReader* reader = IndexReader::open(path.c_str());
                IndexSearcher index_searcher(reader);
                for (int i = 0; i < 1; i++) {
            {
                TimeGuard time("term query1");
                Term* t = _CLNEW Term(_T("description"), _T("telerik"));
                Query* query = _CLNEW TermQuery(t);
                _CLLDECDELETE(t);

                std::vector<int32_t> result;
                index_searcher._search(query,
                                       [&result](const int32_t docid, const float_t /*score*/) {
                                           result.push_back(docid);
                                       });
                std::cout << "term result: " << result.size() << std::endl;
                _CLLDELETE(query);
            }
            {
                TimeGuard time("term query2");
                Term* t = _CLNEW Term(_T("description"), _T("kendo"));
                Query* query = _CLNEW TermQuery(t);
                _CLLDECDELETE(t);

                std::vector<int32_t> result;
                index_searcher._search(query,
                                       [&result](const int32_t docid, const float_t /*score*/) {
                                           result.push_back(docid);
                                       });
                std::cout << "term result: " << result.size() << std::endl;
                _CLLDELETE(query);
            }
            {
                TimeGuard time("term query3");
                Term* t = _CLNEW Term(_T("description"), _T("themes"));
                Query* query = _CLNEW TermQuery(t);
                _CLLDECDELETE(t);

                std::vector<int32_t> result;
                index_searcher._search(query,
                                       [&result](const int32_t docid, const float_t /*score*/) {
                                           result.push_back(docid);
                                       });
                std::cout << "term result: " << result.size() << std::endl;
                _CLLDELETE(query);
            }
            {
                TimeGuard time("phrase query1");
                Term* term1 = _CLNEW Term(_T( "description" ), _T( "telerik" ));
                Term* term2 = _CLNEW Term(_T( "description" ), _T( "kendo" ));
                Term* term3 = _CLNEW Term(_T( "description" ), _T( "themes" ));
                PhraseQuery* query = _CLNEW PhraseQuery();
                query->add(term1);
                query->add(term2);
                query->add(term3);
                _CLLDECDELETE(term1);
                _CLLDECDELETE(term2);
                _CLLDECDELETE(term3);

                std::vector<int32_t> result;
                index_searcher._search(query,
                                       [&result](const int32_t docid, const float_t /*score*/) {
                                           result.push_back(docid);
                                       });
                std::cout << "phrase result: " << result.size() << std::endl;
                _CLLDELETE(query);
            }
            {
                TimeGuard time("phrase query2");
                Term* term1 = _CLNEW Term(_T( "description" ), _T( "telerik" ));
                PhraseQuery* query = _CLNEW PhraseQuery();
                query->add(term1);
                _CLLDECDELETE(term1);

                std::vector<int32_t> result;
                index_searcher._search(query,
                                       [&result](const int32_t docid, const float_t /*score*/) {
                                           result.push_back(docid);
                                       });
                std::cout << "phrase result: " << result.size() << std::endl;
                _CLLDELETE(query);
            }
                }
                reader->close();
                _CLDELETE(reader);
            }
    }

    return 0;
}
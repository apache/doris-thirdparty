#ifndef CLUCENE_CONFIGURATION_H
#define CLUCENE_CONFIGURATION_H

#include <string>

CL_NS_DEF2(analysis, ik)

// TODO(whj): Optimize the design of the Configuration class, remove duplicate configurations (like mode and lowercase)
class Configuration {
private:
    bool use_smart_;
    bool enable_lowercase_;
    std::string dict_path_;

    struct DictFiles {
        std::string main {"main.dic"};
        std::string quantifier {"quantifier.dic"};
        std::string stopwords {"stopword.dic"};
    } dict_files_;

    std::vector<std::string> ext_dict_files_;
    std::vector<std::string> ext_stop_word_dict_files_;

public:
    Configuration() : use_smart_(true), enable_lowercase_(true) {
        ext_dict_files_ = {"extra_main.dic", "extra_single_word.dic", "extra_single_word_full.dic",
                           "extra_single_word_low_freq.dic"};

        ext_stop_word_dict_files_ = {"extra_stopword.dic"};
    }

    bool isUseSmart() const { return use_smart_; }
    Configuration& setUseSmart(bool smart) {
        use_smart_ = smart;
        return *this;
    }

    bool isEnableLowercase() const { return enable_lowercase_; }
    Configuration& setEnableLowercase(bool enable) {
        enable_lowercase_ = enable;
        return *this;
    }

    std::string getDictPath() const { return dict_path_; }
    Configuration& setDictPath(const std::string& path) {
        dict_path_ = path;
        return *this;
    }

    void setMainDictFile(const std::string& file) { dict_files_.main = file; }
    void setQuantifierDictFile(const std::string& file) { dict_files_.quantifier = file; }
    void setStopWordDictFile(const std::string& file) { dict_files_.stopwords = file; }

    const std::string& getMainDictFile() const { return dict_files_.main; }
    const std::string& getQuantifierDictFile() const { return dict_files_.quantifier; }
    const std::string& getStopWordDictFile() const { return dict_files_.stopwords; }

    void addExtDictFile(const std::string& filePath) { ext_dict_files_.push_back(filePath); }
    void addExtStopWordDictFile(const std::string& filePath) {
        ext_stop_word_dict_files_.push_back(filePath);
    }

    const std::vector<std::string>& getExtDictFiles() const { return ext_dict_files_; }
    const std::vector<std::string>& getExtStopWordDictFiles() const {
        return ext_stop_word_dict_files_;
    }
};

CL_NS_END2

#endif //CLUCENE_CONFIGURATION_H

#include "Lexeme.h"

#include <sstream>

CL_NS_DEF2(analysis, ik)

Lexeme::Lexeme(size_t offset, size_t byte_begin, size_t byte_length, Type type, size_t char_begin,
               size_t char_end)
        : offset_(offset),
          byte_begin_(byte_begin),
          byte_length_(byte_length),
          type_(type),
          char_begin_(char_begin),
          char_end_(char_end) {}

void Lexeme::setLength(size_t length) {
    if (length == 0) {
        throw std::invalid_argument("Lexeme length cannot be zero");
    }
    this->byte_length_ = length;
}

void Lexeme::setText(std::string&& text) {
    if (text.empty()) {
        this->text_.clear();
        return;
    }
    this->text_ = std::move(text);
}

bool Lexeme::append(const Lexeme& other, Type new_type) {
    if (this->getByteEndPosition() == other.getByteBeginPosition()) {
        byte_length_ += other.getByteLength();
        char_end_ = other.getCharEnd();
        type_ = new_type;
        return true;
    }
    return false;
}

std::string Lexeme::toString() const {
    std::ostringstream oss;
    oss << getByteBeginPosition() << '-' << getByteEndPosition() << " : " << text_;
    return oss.str();
}

bool Lexeme::operator==(const Lexeme& other) const noexcept {
    return offset_ == other.offset_ && byte_begin_ == other.byte_begin_ &&
           byte_length_ == other.byte_length_;
}

bool Lexeme::operator<(const Lexeme& other) const noexcept {
    return byte_begin_ > other.byte_begin_ ||
           (byte_begin_ == other.byte_begin_ && byte_length_ < other.byte_length_);
}

CL_NS_END2
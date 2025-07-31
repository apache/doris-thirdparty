/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#ifndef _lucene_analysis_AnalysisHeader_
#define _lucene_analysis_AnalysisHeader_

#include "CLucene/index/Payload.h"
#include "CLucene/util/VoidList.h"
#include "CLucene/LuceneThreads.h"
#include "CLucene/util/CLStreams.h"

#include <unordered_set>

CL_CLASS_DEF(util,Reader)
CL_CLASS_DEF(util,IReader)

CL_NS_DEF(analysis)

typedef CL_NS(util)::CLSetList<TCHAR*, CL_NS(util)::Compare::TChar, CL_NS(util)::Deletor::tcArray> CLTCSetList;

/** A Token is an occurence of a term from the text of a field.  It consists of
  a term's text, the start and end offset of the term in the text of the field,
  and a type string.
  <p>
  The start and end offsets permit applications to re-associate a token with
  its source text, e.g., to display highlighted query terms in a document
  browser, or to show matching text fragments in a KWIC (KeyWord In Context)
  display, etc.
  <p>
  The type is an interned string, assigned by a lexical analyzer
  (a.k.a. tokenizer), naming the lexical or syntactic class that the token
  belongs to.  For example an end of sentence marker token might be implemented
  with type "eos".  The default token type is "word".
  <p>
  A Token can optionally have metadata (a.k.a. Payload) in the form of a variable
  length byte array. Use {@link lucene::index::TermPositions#getPayloadLength()} and
  {@link lucene::index::TermPositions#getPayload(byte[], int)} to retrieve the payloads from the index.

  <br><br>
  <p><font color="#FF0000">
  WARNING: The status of the <b>Payloads</b> feature is experimental.
  The APIs introduced here might change in the future and will not be
  supported anymore in such a case.</font>

  <br><br>

  <p>Tokenizers and filters should try to re-use a Token
  instance when possible for best performance, by
  implementing the {@link lucene::index::TokenStream#next(Token)} API.
  Failing that, to create a new Token you should first use
  one of the constructors that starts with null text.  Then
  you should call either {@link #termBuffer()} or {@link
  #resizeTermBuffer(int)} to retrieve the Token's
  termBuffer.  Fill in the characters of your term into this
  buffer, and finally call {@link #setTermLength(int)} to
  set the length of the term text.  See <a target="_top"
  href="https://issues.apache.org/jira/browse/LUCENE-969">LUCENE-969</a>
  for details.</p>

  @see Payload
*/
class CLUCENE_EXPORT Token : LUCENE_BASE {
private:
    int32_t _startOffset;///< start in source text
    int32_t _endOffset;  ///< end in source text
    const TCHAR *_type;  ///< lexical type
    int32_t positionIncrement;
    size_t bufferTextLen;
    void *_buffer;       ///< the text of the term
    int32_t _termTextLen;///< the length of termText. Internal use only
    bool isNoCopy = false;

    CL_NS(index)::Payload *payload;

public:
    static const TCHAR *getDefaultType() { return L"word"; };

    Token() : _startOffset(0),
               _endOffset(0),
               _type(getDefaultType()),
               positionIncrement(1),
               payload(NULL) {
        _termTextLen = 0;
        _buffer = NULL;
        bufferTextLen = 0;
    };
    virtual ~Token() {
        if (!isNoCopy) {
            free(_buffer);
            _buffer = nullptr;
        }
        _CLLDELETE(payload);
    };

    /// Constructs a Token with the given text, start and end offsets, & type.
    //SToken(const T* text, const int32_t start, const int32_t end, const TCHAR* typ=NULL);
    template<typename T>
    void set(const T *text, const int32_t start, const int32_t end, const TCHAR *typ = NULL) {
        _startOffset = start;
        _endOffset = end;
        _type = (typ == NULL ? getDefaultType() : typ);
        positionIncrement = 1;
        setText(text, end - start);
    };

    template<typename T>
    void setNoCopy(const T *text, const int32_t start, const int32_t end, const TCHAR *typ = NULL) {
        _startOffset = start;
        _endOffset = end;
        _type = (typ == NULL ? getDefaultType() : typ);
        positionIncrement = 1;
        setTextNoCopy(text, end - start);
    };

    size_t bufferLength() const {
        return bufferTextLen;
    }
    template<typename T>
    void growBuffer(size_t size) {
        if (bufferTextLen >= size)
            return;
        if (_buffer == NULL) {
            _buffer = (T *) malloc(size * sizeof(T));
            *(T *) _buffer = 0;
        } else {
            //use realloc. growBuffer is public, therefore could be called
            //without a subsequent call to overwriting the memory
            _buffer = (T *) realloc(_buffer, size * sizeof(T));
        }
        bufferTextLen = size;
    }

    template<typename T>
    void *resizeTermBuffer(size_t size) {
        if (bufferTextLen < size)
            growBuffer<T>(size);
        return this->_buffer;
    }
    void setPositionIncrement(int32_t posIncr) {
        if (posIncr < 0) {
            _CLTHROWA(CL_ERR_IllegalArgument, "positionIncrement must be >= 0");
        }
        positionIncrement = posIncr;
    }
    int32_t getPositionIncrement() const { return positionIncrement; }

    template<typename T>
    inline T *termBuffer() const {
        return (T *) _buffer;
    }

    template<typename T>
    inline size_t termLength();//< Length of the the termBuffer. See #termBuffer

    void resetTermTextLen() {
        _termTextLen = -1;
    }

    template<typename T>
    void setText(const T *text, int32_t l) {
        if (bufferTextLen < l + 1)
            growBuffer<T>(l + 1);
        memcpy(_buffer, text, l*sizeof(T));
        _termTextLen = l;
        ((T *) _buffer)[_termTextLen] = 0;//make sure null terminated
    };

    template<typename T>
    void setTextNoCopy(const T *text, int32_t l) {
        _termTextLen = l;
        _buffer = (void*)text;
        isNoCopy = true;
    };

    int32_t startOffset() const {
        return _startOffset;
    }

    void setStartOffset(const int32_t val) {
        _startOffset = val;
    }

    template<typename T>
    void setTermLength(int32_t len) {
        if (bufferTextLen < len)
            this->growBuffer<T>(len);
        this->_termTextLen = len;
    }

    int32_t endOffset() const {
        return _endOffset;
    }

    void setEndOffset(const int32_t val) {
        _endOffset = val;
    }

    const TCHAR *type() const { return _type; }
    void setType(const TCHAR *val) {
        _type = val;
    }

    CL_NS(index)::Payload *getPayload() { return this->payload; }
    void setPayload(CL_NS(index)::Payload *p) {
        _CLLDELETE(this->payload);
        this->payload = p;
    }

    void clear() {
        _CLDELETE(payload);
        // Leave _buffer to allow re-use
        _termTextLen = 0;
        positionIncrement = 1;
        // startOffset = endOffset = 0;
        // type = DEFAULT_TYPE;
    }
};

template <>
inline size_t Token::termLength<char>(){
    if ( _termTextLen == -1 ) //it was invalidated by growBuffer
        _termTextLen = (int32_t)strlen((char*)_buffer);
    return (size_t)_termTextLen;
};

template <>
inline size_t Token::termLength<TCHAR>(){
    if ( _termTextLen == -1 ) //it was invalidated by growBuffer
        _termTextLen = (int32_t)wcslen((TCHAR*)_buffer);
    return (size_t)_termTextLen;
};

class CLUCENE_EXPORT TokenStream {
public:
    /** Returns the next token in the stream, or null at EOS.
	*  When possible, the input Token should be used as the
	*  returned Token (this gives fastest tokenization
	*  performance), but this is not required and a new Token
	*  may be returned (pass NULL for this).
	*  Callers may re-use a single Token instance for successive
	*  calls to this method.
	*  <p>
	*  This implicitly defines a "contract" between
	*  consumers (callers of this method) and
	*  producers (implementations of this method
	*  that are the source for tokens):
	*  <ul>
	*   <li>A consumer must fully consume the previously
	*       returned Token before calling this method again.</li>
	*   <li>A producer must call {@link Token#clear()}
	*       before setting the fields in it & returning it</li>
	*  </ul>
	*  Note that a {@link TokenFilter} is considered a consumer.
	*  @param result a Token that may or may not be used to return
	*  @return next token in the stream or null if end-of-stream was hit
	*/
    virtual Token* next(Token* token) = 0;

    /** Releases resources associated with this stream. */
    virtual void close() = 0;

    /** Resets this stream to the beginning. This is an
   *  optional operation, so subclasses may or may not
   *  implement this method. Reset() is not needed for
   *  the standard indexing process. However, if the Tokens
   *  of a TokenStream are intended to be consumed more than
   *  once, it is necessary to implement reset().
   */
    virtual void reset(){};

    virtual ~TokenStream(){};
};


class CLUCENE_EXPORT Analyzer{
public:
    Analyzer();

    virtual bool isSDocOpt() { return false; }

    virtual void initDict(const std::string &dictPath) {}

    /** Creates a TokenStream which tokenizes all the text in the provided
	Reader.  Default implementation forwards to tokenStream(Reader) for
	compatibility with older version.  Override to allow Analyzer to choose
	strategy based on document and/or field.  Must be able to handle null
	field name for backward compatibility. */
    virtual TokenStream* tokenStream(const TCHAR* fieldName, CL_NS(util)::Reader* reader)=0;

    /** Creates a TokenStream that is allowed to be re-used
	*  from the previous time that the same thread called
	*  this method.  Callers that do not need to use more
	*  than one TokenStream at the same time from this
	*  analyzer should use this method for better
	*  performance.
	*/
    virtual TokenStream* reusableTokenStream(const TCHAR* fieldName, CL_NS(util)::Reader* reader);

    virtual void set_lowercase(bool lowercase) {
        _lowercase = lowercase;
    }

    virtual void set_stopwords(std::unordered_set<std::string_view>* stopwords) {
        _stopwords = stopwords;
    }

    virtual void set_ownReader(bool ownReader) {
        _ownReader = ownReader;
    }

protected:

    /** Used by Analyzers that implement reusableTokenStream
	*  to retrieve previously saved TokenStreams for re-use
	*  by the same thread. */
    virtual TokenStream* getPreviousTokenStream() {
        return prev_tokenStream;
    }

    /** Used by Analyzers that implement reusableTokenStream
	*  to save a TokenStream for later re-use by the same
	*  thread. */
    virtual void setPreviousTokenStream(TokenStream* obj) {
        prev_tokenStream = obj;
    }
    bool _lowercase = false;
    bool _ownReader = false;
    std::unordered_set<std::string_view>* _stopwords = nullptr;
    TokenStream* prev_tokenStream{};

public:
    /**
	* Invoked before indexing a Field instance if
	* terms have already been added to that field.  This allows custom
	* analyzers to place an automatic position increment gap between
	* Field instances using the same field name.  The default value
	* position increment gap is 0.  With a 0 position increment gap and
	* the typical default token position increment of 1, all terms in a field,
	* including across Field instances, are in successive positions, allowing
	* exact PhraseQuery matches, for instance, across Field instance boundaries.
	*
	* @param fieldName Field name being indexed.
	* @return position increment gap, added to the next token emitted from {@link #tokenStream(TCHAR*, Reader*)}
	*/
    virtual int32_t getPositionIncrementGap(const TCHAR* fieldName){return 0;}

    virtual ~Analyzer();
};

/** An Analyzer builds TokenStreams, which analyze text.  It thus represents a
 *  policy for extracting index terms from text.
 *  <p>
 *  Typical implementations first build a Tokenizer, which breaks the stream of
 *  characters from the Reader into raw Tokens.  One or more TokenFilters may
 *  then be applied to the output of the Tokenizer.
 *  <p>
 *  WARNING: You must override one of the methods defined by this class in your
 *  subclass or the Analyzer will enter an infinite loop.
 */

class CLUCENE_EXPORT Tokenizer:public TokenStream {
protected:
    /** The text source for this Tokenizer. */
    CL_NS(util)::Reader* input;
    bool lowercase = false;
    bool ownReader = false;
    std::unordered_set<std::string_view>* stopwords = nullptr;

public:
    /** Construct a tokenizer with null input. */
    Tokenizer():input(nullptr){}
    /** Construct a token stream processing the given input. */
    explicit Tokenizer(CL_NS(util)::Reader* _input, bool _ownReader = false):input(_input), ownReader(_ownReader){}

    /** By default, closes the input Reader. */
    virtual void close() {
        if (input != NULL) {
            if (ownReader) {
                _CLDELETE(input);
            } else {
                input = NULL;
            }
        }
    };

    /** Expert: Reset the tokenizer to a new reader.  Typically, an
	*  analyzer (in its reusableTokenStream method) will use
	*  this to re-use a previously created tokenizer. */
    using TokenStream::reset;
    virtual void reset(CL_NS(util)::Reader* _input) {
        // ? delete input;
        this->input = _input;
    };

    virtual ~Tokenizer() {
        close();
    };
};

/** A TokenFilter is a TokenStream whose input is another token stream.
<p>
This is an abstract class.
*/
class CLUCENE_EXPORT TokenFilter:public TokenStream {
protected:
    /** The source of tokens for this filter. */
	TokenStream* input;
    /** If true then input will be deleted in the destructor */
	bool deleteTokenStream;

    /** Construct a token stream filtering the given input.
     *
     * @param in The TokenStream to filter from
     * @param deleteTS If true, input will be deleted in the destructor
    */
	TokenFilter(TokenStream* in, bool deleteTS=false);
	virtual ~TokenFilter();
public:
    /** Close the input TokenStream. */
	void close();
};

CL_NS_END
#endif

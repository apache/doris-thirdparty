/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
* 
* Distributable under the terms of either the Apache License (Version 2.0) or 
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#ifndef _lucene_util_CLStreams_
#define _lucene_util_CLStreams_

#include <algorithm>

#include "CLucene/debug/error.h"

CL_NS_DEF(util)

class IReader {
public:
    virtual ~IReader() = default;
    virtual int32_t read(const void **start, int32_t min, int32_t max) = 0;
    virtual int64_t skip(int64_t ntoskip) = 0;
    virtual int64_t position() = 0;
    virtual size_t size() = 0;
};

template <typename T>
class CLUCENE_EXPORT CLStream: public IReader{
public:
	virtual ~CLStream(){}

    virtual void init(const void *_value, int32_t _length, bool copyData) {
        _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException: CLStream::init");
    }

	inline int read(){
		const T* buffer;
		const int32_t nread = read((const void**)&buffer,1, 1);
		if ( nread < 0 )
			return -1;
		else
			return buffer[0];
	}

	/** Read one line, return the length of the line read.
	* If the string is longer than len, only len of that line will be copied
	*/
	inline int32_t readLine(T* buffer, size_t len){
		size_t i = 0;
		while (true && i<len-1) {
			int32_t b = read();
			if (b < 1)
				break;
			if (b == '\n' || b == '\r') {
				if (i > 0)
					break;
				else
					continue;
			}
			buffer[i++] = b;
		}
		buffer[i] = 0;
		return i;
	}
	
    /**
     * @brief Reads items from the stream and sets @p start to point to
     * the first item that was read.
     *
     * Note: unless stated otherwise in the documentation for that method,
     * this pointer will no longer be valid after calling another method of
     * this class. The pointer will also no longer be valid after the class
     * is destroyed.
     *
     * At least @p min items will be read from the stream, unless an error occurs
     * or the end of the stream is reached.  Under no circumstances will more than
     * @p max items be read.
     *
     * If the end of the stream is reached before @p min items are read, the
     * read is still considered successful and the number of items read will
     * be returned.
     *
     * @param start pointer passed by reference that will be set to point to
     *              the retrieved array of items. If the end of the stream
     *              is encountered or an error occurs, the value of @p start
     *              is undefined
     * @param min   the minimal number of items to read from the stream. This
     *              value should be larger than 0. If it is 0 or smaller, the
     *              result is undefined
     * @param max   the maximal number of items to read from the stream.
     *              If this value is smaller than @p min, there is no limit on
     *              the number of items that can be read
     * @return the number of items that were read. @c -1 is returned if
     *         end of the stream has already been reached. An error is thrown
	 *			if an error occurs.
     **/
	virtual int32_t read(const void ** start, int32_t min, int32_t max) = 0;
    virtual int32_t readCopy(void* start, int32_t off, int32_t len) {
        _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException: CLStream::read");
    }
    /**
     * @brief Skip @p ntoskip items.
     *
     * If an error occurs, or the end of the stream is encountered, fewer
     * than @p ntoskip items may be skipped.  This can be checked by comparing
     * the return value to @p ntoskip.
     *
     * Calling this function invalidates the data pointer that was obtained from
     * StreamBase::read.
     *
     * @param ntoskip the number of items that should be skipped
     * @return the number of items skipped
     **/
	virtual int64_t skip(int64_t ntoskip) = 0;
    /**
     * @brief Get the current position in the stream.
     * The value obtained from this function can be used to reset the stream.
     **/
	virtual int64_t position() = 0;
	int64_t getPosition(){ return this->position(); }

	virtual size_t size() = 0;
};

template <class T> 
class CLUCENE_EXPORT BufferedStream{
public:
		virtual ~BufferedStream(){}
    /**
     * @brief Repositions this stream to a given position.
     *
     * A call to reset is only guaranteed to be successful when
     * the requested position lies within the segment of a stream
     * corresponding to a valid pointer obtained from read.
     * In this case, the pointer will not be invalidated.
     *
     * Calling this function invalidates the data pointer that was obtained from
     * StreamBase::read unless the conditions outlined above apply.
     *
     * To read n items, leaving the stream at the same position as before, you
     * can do the following:
     * @code
     * int64_t start = stream.position();
     * if ( stream.read(data, min, max) > 0 ) {
     *     stream.reset(start);
     *     // The data pointer is still valid here
     * }
     * @endcode
     *
     * @param pos the position in the stream you want to go to, relative to
     * the start of the stream
     * @return the new position in the stream
     **/
    virtual int64_t reset(int64_t) = 0;
    /**
     * @brief Sets the minimum size of the buffer
     */
	virtual void setMinBufSize(int32_t s) = 0;
};

class BufferedReader;
class CLUCENE_EXPORT Reader: public CLStream<TCHAR>{
public:
	~Reader(){}
	virtual BufferedReader* __asBufferedReader(){ return NULL; }
};
class CLUCENE_EXPORT BufferedReader: public Reader, public BufferedStream<TCHAR>{
public:
	_CL_DEPRECATED( setMinBufSize ) int64_t mark(int32_t readAheadlimit){
		this->setMinBufSize(readAheadlimit);
		return this->position();
	}
	~BufferedReader(){}
	BufferedReader* __asBufferedReader(){ return this; }
};
typedef CLStream<signed char> InputStream;
class CLUCENE_EXPORT BufferedInputStream: public InputStream, public BufferedStream<signed char>{
public:
	virtual ~BufferedInputStream(){}
};
	
template<typename T>
class CLUCENE_EXPORT SStringReader : public Reader {
protected:
    const T *value;
    bool ownValue{};
    int64_t pos{};
    size_t m_size{};
    size_t buffer_size{};

public:
    SStringReader(): value(NULL), ownValue(false),pos(0),m_size(0),buffer_size(0){};
    SStringReader(const T *_value, const int32_t _length, bool copyData=true) {
        this->m_size = 0;
        this->value = NULL;
        this->ownValue = true;
        this->buffer_size = 0;
        this->init(_value, _length, copyData);
    }

    // _value should be type T*
    void init(const void *_value, int32_t _length, bool copyData = true) override {
        const size_t length = (size_t)_length;
        this->pos = 0;
        if (copyData) {
            T *tmp = (T *) this->value;
            if (tmp == NULL || !this->ownValue) {
                tmp = _CL_NEWARRAY(T, length + 1);
                this->buffer_size = length;
            } else if (length > this->buffer_size || length < (this->buffer_size / 2)) {//expand, or shrink
                tmp = (T *) realloc(tmp, sizeof(T) * (length + 1));
                this->buffer_size = length;
            }
            // copy data
            memcpy(tmp, _value, length * sizeof(T));
            // add trailing zero
            tmp[length] = 0;
            this->value = tmp;
        } else {
            if (ownValue && this->value != NULL) {
                _CLDELETE_LARRAY((T *) this->value);
            }
            this->value = (T *)_value;
            this->buffer_size = 0;
        }
        this->m_size = length;
        this->ownValue = copyData;
    };
    virtual ~SStringReader() {
        if (ownValue && this->value != NULL) {
            auto *v = (T *) this->value;
            _CLDELETE_LARRAY(v);
            this->value = NULL;
        }
    }

    // for test only
    int testValueAt(const size_t i) {
        if (i <= this->m_size) {
            return this->value[i];
        } else {
            return -1;
        }
    }

    int32_t read(const void **start, int32_t min, int32_t max) override {
        if (m_size == pos)
            return -1;
        *(const T**)start = this->value + pos;
        auto tmp = std::max(min, max);
        int32_t r = (int32_t) std::min(tmp, static_cast<int32_t>(m_size - pos));
        pos += r;
        return r;
    }

    int32_t readCopy(void* start, int32_t off, int32_t len) override {
        if (len == 0) return 0;
        if (pos >= m_size) return -1;
        int32_t n = std::min(static_cast<int32_t>(m_size - pos), len);
        std::copy_n(value + pos, n, static_cast<T*>(start) + off);
        pos += n;
        return n;
    }

    int64_t position() override {
        return pos;
    }
    int64_t reset(int64_t new_pos){
        if ( new_pos >= 0 && new_pos < this->m_size )
            this->pos = new_pos;
        return this->pos;
    }
    int64_t skip(int64_t ntoskip) override{
        int64_t s = std::min(ntoskip, (int64_t)m_size-pos);
        this->pos += s;
        return s;
    }
    size_t size() override {
        return m_size;
    }
};

class CLUCENE_EXPORT FilteredBufferedReader: public BufferedReader{
	class Internal;
	Internal* _internal;
public:
	FilteredBufferedReader(Reader* reader, bool deleteReader);
	virtual ~FilteredBufferedReader();
    
    void init(const void *_value, int32_t _length, bool copyData) override {
        _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException: CLStream::init");
    }

	int32_t read(const void** start, int32_t min, int32_t max) override;
	int64_t position() override;
	int64_t reset(int64_t) override;
	int64_t skip(int64_t ntoskip) override;
	size_t size() override;
	void setMinBufSize(int32_t minbufsize) override;
};

class CLUCENE_EXPORT FilteredBufferedInputStream: public BufferedInputStream{
	class Internal;
	Internal* _internal;
public:
	FilteredBufferedInputStream(InputStream* input, bool deleteInput);
	virtual ~FilteredBufferedInputStream();
	
	int32_t read(const void ** start, int32_t min, int32_t max);
	int64_t position();
	int64_t reset(int64_t);
	int64_t skip(int64_t ntoskip);
	size_t size();
	void setMinBufSize(int32_t minbufsize);
};


class CLUCENE_EXPORT StringReader: public BufferedReader{
protected:
	const TCHAR* value;
	bool ownValue;
	int64_t pos;
	size_t m_size;
  size_t buffer_size;
public:
  StringReader ( const TCHAR* value, const int32_t length = -1, bool copyData = true );
  void init ( const void* value, const int32_t length, bool copyData = true ) override;
	virtual ~StringReader();

  int32_t read(const void** start, int32_t min, int32_t max) override;
  int64_t position() override;
  int64_t reset(int64_t) override;
	int64_t skip(int64_t ntoskip) override;
	void setMinBufSize(int32_t s) override;
	size_t size() override;
};
class CLUCENE_EXPORT AStringReader: public BufferedInputStream{
	signed char* value;
	bool ownValue;
	int64_t pos;
protected:
	size_t m_size;
public:
    AStringReader ( const char* value, const int32_t length = -1 );
    AStringReader ( char* value, const int32_t length, bool copyData = true );
	 virtual ~AStringReader();
	
    int32_t read(const signed char*& start, int32_t min, int32_t max);
    int32_t read(const void** start, int32_t min, int32_t max);

    int32_t read(const unsigned char*& start, int32_t min, int32_t max);
    int64_t position();
    int64_t reset(int64_t);
	int64_t skip(int64_t ntoskip);
	void setMinBufSize(int32_t s);
	size_t size();
};

/**
* A helper class which constructs a FileReader with a specified
* simple encodings, or a given inputstreamreader
*/
class CLUCENE_EXPORT FileInputStream: public BufferedInputStream {
	class Internal;
	Internal* _internal;
protected:
	void init(InputStream *i, int encoding);

    void init(const void *_value, int32_t _length, bool copyData) override {
        _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException: CLStream::init");
    }
public:
	LUCENE_STATIC_CONSTANT(int32_t, DEFAULT_BUFFER_SIZE=4096);
	FileInputStream ( const char* path, int32_t buflen = -1 );
	virtual ~FileInputStream ();
	
	int32_t read(const void** start, int32_t min, int32_t max) override;
	int64_t position() override;
	int64_t reset(int64_t) override;
	int64_t skip(int64_t ntoskip) override;
	size_t size() override;
	void setMinBufSize(int32_t minbufsize) override;
};

class CLUCENE_EXPORT SimpleInputStreamReader: public BufferedReader{
	class Internal;
	Internal* _internal;
protected:
	void init(InputStream *i, int encoding);

    void init(const void *_value, int32_t _length, bool copyData) override {
        _CLTHROWA(CL_ERR_UnsupportedOperation, "UnsupportedOperationException: CLStream::init");
    }
public:	
	enum{
		ASCII=1,
		UTF8=2,
		UCS2_LE=3
	};
	
	SimpleInputStreamReader();
   SimpleInputStreamReader(InputStream *i, int encoding);
	virtual ~SimpleInputStreamReader();
	
  int32_t read(const void** start, int32_t min, int32_t max) override;
  int64_t position() override;
  int64_t reset(int64_t) override;
  int64_t skip(int64_t ntoskip) override;
  void setMinBufSize(int32_t s) override;
  size_t size() override;
};

/**
* A helper class which constructs a FileReader with a specified
* simple encodings, or a given inputstreamreader.
* It is recommended that you use the contribs package for proper 
* decoding using iconv. This class is provided only as a dependency-less
* replacement.
*/
class CLUCENE_EXPORT FileReader: public SimpleInputStreamReader{
public:
	FileReader(const char* path, int encoding, int32_t buflen = -1);
	FileReader(const char* path, const char* encoding, int32_t buflen = -1);
	virtual ~FileReader();
};

CL_NS_END

#define jstreams CL_NS(util)

#endif

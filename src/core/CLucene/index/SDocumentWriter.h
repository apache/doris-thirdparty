//
// Created by 姜凯 on 2022/9/16.
//

#ifndef _lucene_index_SDocumentsWriter_
#define _lucene_index_SDocumentsWriter_

#include "CLucene/_ApiHeader.h"
#include "CLucene/index/IndexWriter.h"
#include "CLucene/store/Directory.h"
#include "CLucene/store/_RAMDirectory.h"
#include "CLucene/util/Array.h"
#include "_DocumentsWriter.h"
#include "_FieldInfos.h"
#include "_TermInfo.h"
#include "_TermInfosWriter.h"

CL_CLASS_DEF(analysis, Analyzer)
CL_CLASS_DEF(analysis, Token)
CL_CLASS_DEF(analysis, TokenStream)
CL_CLASS_DEF(document, Document)
CL_CLASS_DEF(document, Field)
CL_CLASS_DEF(util, StringReader)

CL_NS_DEF(index)
class IndexWriter;
class FieldInfo;
class FieldInfos;
class TermInfo;
class TermInfosWriter;
class ByteBlockPool;
class SCharBlockPool;
class DefaultSkipListWriter;

typedef CL_NS(util)::StringReader ReusableStringReader;

template<typename T>
class SDocumentsWriter : public IDocumentsWriter {
public:
private:
    IndexWriter *writer{};
    CL_NS(store)::Directory *directory{};
    FieldInfos *fieldInfos;// All fields we've seen
    int32_t nextDocID;     // Next docID to be added
    int32_t numDocsInRAM;  // # docs buffered in RAM
    int32_t numDocsInStore;// # docs written to doc stores
    int32_t nextWriteDocID;// Next docID to be written
    bool flushPending{};   // True when a thread has decided to flush
    bool bufferIsFull{};   // True when it's time to write segment
    bool hasNorms{};
    bool closed{};
    std::string segment;// Current segment we are working on
    std::vector<uint32_t> docDeltaBuffer;
    std::vector<uint32_t> freqBuffer;
    std::vector<uint32_t> posBuffer;
    std::ostream* infoStream{};
    int64_t ramBufferSize;
    // Flush @ this number of docs.  If rarmBufferSize is
    // non-zero we will flush by RAM usage instead.
    int32_t maxBufferedDocs;
    bool hasProx_ = false;
    IndexVersion indexVersion_ = IndexVersion::kV1;
    uint32_t flags_ = 0;

public:
    class FieldMergeState;

    struct Posting {
        int32_t textStart;   // Address into char[] blocks where our text is stored
        int32_t textLen;     // our text length
        int32_t docFreq = 0;     // # times this term occurs in the current doc
        int32_t freqStart;   // Address of first uint8_t[] slice for freq
        int32_t freqUpto;    // Next write address for freq
        int32_t proxStart = 0;   // Address of first uint8_t[] slice
        int32_t proxUpto = 0;    // Next write address for prox
        int32_t lastDocID;   // Last docID where this term occurred
        int32_t lastDocCode; // Code for prior doc
        int32_t lastPosition;// Last position where this term occurred
        std::basic_string_view<T> term_;
    };

    /* Stores norms, buffered in RAM, until they are flushed
   * to a partial segment. */
    class BufferedNorms {
    public:
        CL_NS(store)::RAMOutputStream out;
        int32_t upto{};

        BufferedNorms();
        void add(float_t norm);
        void reset();
        void fill(int32_t docID);
    };
    template<typename T2>
    class BlockPool {
    protected:
        bool trackAllocations{};

        int32_t numBuffer;

        int32_t bufferUpto;// Which buffer we are upto
        int32_t blockSize{};

        SDocumentsWriter *parent;

    public:
        CL_NS(util)::ValueArray<T2 *> buffers;
        int32_t tOffset;// Current head offset
        int32_t tUpto;  // Where we are in head buffer
        T2 *buffer;     // Current head buffer

        virtual T2 *getNewBlock(bool trackAllocations) = 0;

        BlockPool(SDocumentsWriter *_parent, int32_t _blockSize, bool trackAllocations) : buffers(CL_NS(util)::ValueArray<T2 *>(10)) {
            this->blockSize = _blockSize;
            this->parent = _parent;
            bufferUpto = -1;
            tUpto = blockSize;
            tOffset = -blockSize;
            buffer = nullptr;
            numBuffer = 0;
            this->trackAllocations = trackAllocations;
            buffer = nullptr;
        }
        virtual ~BlockPool() {
            buffers.deleteValues();
        }

        virtual void reset() = 0;

        void nextBuffer() {
            if (1 + bufferUpto == buffers.length) {
                //expand the number of buffers
                buffers.resize((int32_t) (buffers.length * 1.5));
            }
            buffer = buffers.values[1 + bufferUpto] = getNewBlock(trackAllocations);
            bufferUpto++;

            tUpto = 0;
            tOffset += blockSize;
        }
    };
    class SCharBlockPool : public BlockPool<T> {
    public:
        explicit SCharBlockPool(SDocumentsWriter *_parent) : BlockPool<T>(_parent, CHAR_BLOCK_SIZE, false) {}
        virtual ~SCharBlockPool() = default;
        T *getNewBlock(bool trackAllocations) override;
        void reset() override;
    };
    class ByteBlockPool : public BlockPool<uint8_t> {
    public:
        ByteBlockPool(bool _trackAllocations, SDocumentsWriter *_parent) : BlockPool<uint8_t>(_parent, BYTE_BLOCK_SIZE, _trackAllocations) {}
        ~ByteBlockPool() override {
            reset();
            //delete the first block
            _CLDELETE_ARRAY(BlockPool<uint8_t>::buffer);
        }
        uint8_t *getNewBlock(bool trackAllocations) override {
            return BlockPool<uint8_t>::parent->getByteBlock(trackAllocations);
        }
        int32_t newSlice(const int32_t size) {
            if (BlockPool<uint8_t>::tUpto > BYTE_BLOCK_SIZE - size)
                BlockPool<uint8_t>::nextBuffer();
            const int32_t upto = BlockPool<uint8_t>::tUpto;
            BlockPool<uint8_t>::tUpto += size;
            BlockPool<uint8_t>::buffer[BlockPool<uint8_t>::tUpto - 1] = 16;
            return upto;
        }
        int32_t allocSlice(uint8_t *slice, const int32_t upto) {
            const int32_t level = slice[upto] & 15;
            assert(level < 10);
            const int32_t newLevel = nextLevelArray[level];
            const int32_t newSize = levelSizeArray[newLevel];

            // Maybe allocate another block
            if (BlockPool<uint8_t>::tUpto > BYTE_BLOCK_SIZE - newSize)
                BlockPool<uint8_t>::nextBuffer();

            const int32_t newUpto = BlockPool<uint8_t>::tUpto;
            const uint32_t offset = newUpto + BlockPool<uint8_t>::tOffset;
            BlockPool<uint8_t>::tUpto += newSize;

            // Copy forward the past 3 bytes (which we are about
            // to overwrite with the forwarding address):
            BlockPool<uint8_t>::buffer[newUpto] = slice[upto - 3];
            BlockPool<uint8_t>::buffer[newUpto + 1] = slice[upto - 2];
            BlockPool<uint8_t>::buffer[newUpto + 2] = slice[upto - 1];

            // Write forwarding address at end of last slice:
            slice[upto - 3] = (uint8_t) (offset >> 24);//offset is unsigned...
            slice[upto - 2] = (uint8_t) (offset >> 16);
            slice[upto - 1] = (uint8_t) (offset >> 8);
            slice[upto] = (uint8_t) offset;

            // Write new level:
            BlockPool<uint8_t>::buffer[BlockPool<uint8_t>::tUpto - 1] = (uint8_t) (16 | newLevel);

            return newUpto + 3;
        }
        void reset() override {
            if (BlockPool<uint8_t>::bufferUpto != -1) {
                // We allocated at least one buffer

                for (int i = 0; i < BlockPool<uint8_t>::bufferUpto; i++)
                    // Fully zero fill buffers that we fully used
                    memset(BlockPool<uint8_t>::buffers.values[i], 0, BYTE_BLOCK_SIZE);

                // Partial zero fill the final buffer
                memset(BlockPool<uint8_t>::buffers.values[BlockPool<uint8_t>::bufferUpto], 0, BlockPool<uint8_t>::tUpto);

                if (BlockPool<uint8_t>::bufferUpto > 0)
                    // Recycle all but the first buffer
                    BlockPool<uint8_t>::parent->recycleBlocks(BlockPool<uint8_t>::buffers, 1, 1 + BlockPool<uint8_t>::bufferUpto);

                // Re-use the first buffer
                BlockPool<uint8_t>::bufferUpto = 0;
                BlockPool<uint8_t>::tUpto = 0;
                BlockPool<uint8_t>::tOffset = 0;
                BlockPool<uint8_t>::buffer = BlockPool<uint8_t>::buffers[0];
            }
        }
    };
    class ThreadState {
    public:
        class FieldData : public CL_NS(util)::Comparable {
        private:
            ThreadState *threadState;

            int32_t fieldCount;
            CL_NS(util)::ValueArray<CL_NS(document)::Field *> docFields;

            FieldData *next;

            bool postingsCompacted;

            CL_NS(util)::ValueArray<Posting *> postingsHash;
            int32_t postingsHashSize;
            int32_t postingsHashHalfSize;
            int32_t postingsHashMask;
            int32_t postingsHashConflicts{};

            int32_t postingsVectorsUpto;
            SDocumentsWriter *_parent;

            int32_t offsetEnd;
            CL_NS(analysis)::Token *localSToken;

            int32_t offsetStartCode;
            int32_t offsetStart;

            void initPostingArrays();
            void addPosition(CL_NS(analysis)::Token *token);
            void rehashPostings(int32_t newSize);
            void compactPostings() {
                int32_t upto = 0;
                for (int32_t i = 0; i < postingsHashSize; i++)
                    if (postingsHash[i] != NULL)
                        postingsHash.values[upto++] = postingsHash[i];

                assert(upto == numPostings);
                postingsCompacted = true;
            }

        public:
            int32_t numPostings;
            FieldInfo *fieldInfo;
            int32_t lastGen;
            int32_t position;
            int32_t length;
            int32_t offset;
            float_t boost;
            bool doNorms;
            void resetPostingArrays() {
                if (!postingsCompacted)
                    compactPostings();
                _parent->recyclePostings(this->postingsHash, numPostings);
                memset(postingsHash.values, 0, postingsHash.length * sizeof(Posting *));
                postingsCompacted = false;
                numPostings = 0;
            }

            FieldData(SDocumentsWriter *_parent, ThreadState *__threadState, FieldInfo *fieldInfo);
            ~FieldData();
            /** So Arrays.sort can sort us. */
            //int32_t compareTo(const void* o);

            /** Collapse the hash table & sort in-place. */
            CL_NS(util)::ValueArray<Posting *> *sortPostings() {
                compactPostings();
                threadState->doPostingSort(postingsHash.values, numPostings);
                return &postingsHash;
            }

            void processField(CL_NS(analysis)::Analyzer *sanalyzer);

            void invertField(CL_NS(document)::Field *field, CL_NS(analysis)::Analyzer *sanalyzer, int32_t maxFieldLength);

            static bool sort(FieldData *e1, FieldData *e2) {
                return _tcscmp(e1->fieldInfo->name, e2->fieldInfo->name) < 0;
            }

            const char *getObjectName() const override {
                return getClassName();
            }
            static const char *getClassName() {
                return "SDocumentsWriter::ThreadState";
            }
            int32_t compareTo(lucene::util::NamedObject *) override;
            friend class ThreadState;
            friend class FieldMergeState;
        };

    private:
        CL_NS(util)::ValueArray<Posting *> postingsFreeListTS;// Free Posting instances
        int32_t postingsFreeCountTS;

        CL_NS(util)::ValueArray<int64_t> vectorFieldPointers;
        CL_NS(util)::ValueArray<int32_t> vectorFieldNumbers;

        int32_t numStoredFields{};// How many stored fields in current doc
        float_t docBoost{};       // Boost for current doc

        CL_NS(util)::ValueArray<FieldData *> fieldDataArray;// Fields touched by current doc
        int32_t numFieldData{};                               // How many fields in current doc
        int32_t numVectorFields;                            // How many vector fields in current doc

        CL_NS(util)::ValueArray<FieldData *> fieldDataHash;// Hash FieldData instances by field name
        int32_t fieldDataHashMask;
        TCHAR *maxTermPrefix{};// Non-null prefix of a too-large term if this
                             // doc has one
        char *maxStermPrefix{};

        int32_t fieldGen{};

        // Used to read a string value for a field
        ReusableStringReader *stringReader;


        ByteBlockPool *postingsPool;
        SCharBlockPool *scharPool;

        // Current posting we are working on
        Posting *p;

        //writeFreqByte...
        uint8_t *freq{};
        int32_t freqUpto{};

        //writeProxByte...
        uint8_t *prox{};
        int32_t proxUpto{};

        //writeOffsetByte...
        uint8_t *offsets{};
        int32_t offsetUpto{};

        //writePosByte...
        uint8_t *pos{};
        int32_t posUpto{};


        /** Do in-place sort of Posting array */
        void doPostingSort(Posting **postings, int32_t numPosting) {
            quickSort(postings, 0, numPosting - 1);
        }

        void quickSort(Posting **postings, int32_t lo, int32_t hi);

        bool postingEquals(const T *tokenText, int32_t tokenTextLen);

        /** Compares term text for two Posting instance and
      *  returns -1 if p1 < p2; 1 if p1 > p2; else 0.
      */
        int32_t comparePostings(Posting *p1, Posting *p2);


    public:
        int32_t numThreads;// Number of threads that use this instance
        int32_t numAllFieldData{};
        CL_NS(util)::ValueArray<FieldData *> allFieldDataArray;// All FieldData instances
        bool doFlushAfter{};
        int32_t docID{};// docID we are now working on

        SDocumentsWriter *_parent;

        ThreadState(SDocumentsWriter *_parent);
        virtual ~ThreadState();

        /** Initializes shared state for this new document */
        void init(CL_NS(document)::Document *doc, int32_t docID);

        /** Tokenizes the fields of a document into Postings */
        void processDocument(CL_NS(analysis)::Analyzer *sanalyzer);

        void resetCurrentFieldData(CL_NS(document)::Document *doc);

        /** If there are fields we've seen but did not see again
      *  in the last run, then free them up.  Also reduce
      *  postings hash size. */
        void trimFields();

        /** Clear the postings hash and return objects back to
      *  shared pool */
        void resetPostings();

        /** Move all per-document state that was accumulated in
      *  the ThreadState into the "real" stores. */
        void writeDocument();

        /** Write vInt into freq stream of current Posting */
        void writeFreqVInt(int32_t i);

        /** Write vInt into prox stream of current Posting */
        void writeProxVInt(int32_t i);

        /** Write uint8_t into freq stream of current Posting */
        void writeFreqByte(uint8_t b);

        /** Write uint8_t into prox stream of current Posting */
        void writeProxByte(uint8_t b);

        /** Currently only used to copy a payload into the prox
      *  stream. */
        void writeProxBytes(const uint8_t *b, int32_t offset, int32_t len);

        /** Write uint8_t into offsets stream of current
      *  PostingVector */
        //void writeOffsetByte(uint8_t b);

        /** Write uint8_t into pos stream of current
      *  PostingVector */
        //void writePosByte(uint8_t b);
        friend class FieldMergeState;
    };

    class ByteSliceReader : public CL_NS(store)::IndexInput {
        ByteBlockPool *pool;
        int32_t bufferUpto;
        const uint8_t *buffer;
        int32_t limit;
        int32_t level;

        int32_t upto;
        int32_t bufferOffset;
        int32_t endIndex;

    public:
        ByteSliceReader() {
            pool = NULL;
            bufferUpto = 0;
            buffer = 0;
            limit = 0;
            level = 0;
            upto = 0;
            bufferOffset = 0;
            endIndex = 0;
        }
        virtual ~ByteSliceReader(){};
        void init(ByteBlockPool *_pool, int32_t _startIndex, int32_t _endIndex) {
            assert(_endIndex - _startIndex > 0);

            level = 0;
            this->pool = _pool;
            this->endIndex = _endIndex;

            bufferUpto = _startIndex / BYTE_BLOCK_SIZE;
            bufferOffset = bufferUpto * BYTE_BLOCK_SIZE;
            buffer = pool->buffers[bufferUpto];
            upto = _startIndex & BYTE_BLOCK_MASK;

            const int32_t firstSize = levelSizeArray[0];

            if (_startIndex + firstSize >= endIndex) {
                // There is only this one slice to read
                limit = endIndex & BYTE_BLOCK_MASK;
            } else
                limit = upto + firstSize - 4;
        }

        uint8_t readByte() override {
            // Assert that we are not @ EOF
            assert(upto + bufferOffset < endIndex);
            if (upto == limit)
                nextSlice();
            return buffer[upto++];
        }

        int64_t writeTo(CL_NS(store)::IndexOutput *out) {
            int64_t size = 0;
            while (true) {
                if (limit + bufferOffset == endIndex) {
                    assert(endIndex - bufferOffset >= upto);
                    out->writeBytes(buffer + upto, limit - upto);
                    size += limit - upto;
                    break;
                } else {
                    out->writeBytes(buffer + upto, limit - upto);
                    size += limit - upto;
                    nextSlice();
                }
            }

            return size;
        }
        void nextSlice() {

            // Skip to our next slice
            const int32_t nextIndex = ((buffer[limit] & 0xff) << 24) + ((buffer[1 + limit] & 0xff) << 16) + ((buffer[2 + limit] & 0xff) << 8) + (buffer[3 + limit] & 0xff);
            level = nextLevelArray[level];
            const int32_t newSize = levelSizeArray[level];

            bufferUpto = nextIndex / BYTE_BLOCK_SIZE;
            bufferOffset = bufferUpto * BYTE_BLOCK_SIZE;

            buffer = pool->buffers[bufferUpto];
            upto = nextIndex & BYTE_BLOCK_MASK;

            if (nextIndex + newSize >= endIndex) {
                // We are advancing to the const slice
                assert(endIndex - nextIndex > 0);
                limit = endIndex - bufferOffset;
            } else {
                // This is not the const slice (subtract 4 for the
                // forwarding address at the end of this new slice)
                limit = upto + newSize - 4;
            }
        }
        void readBytes(uint8_t *b, const int32_t len) override {
            readBytes(b, len, 0);
        }
        void readBytes(uint8_t *b, int32_t len, int32_t offset) override {
            while (len > 0) {
                const int32_t numLeft = limit - upto;
                if (numLeft < len) {
                    // Read entire slice
                    memcpy(b + offset, buffer + upto, numLeft * sizeof(uint8_t));
                    b += numLeft;
                    len -= numLeft;
                    nextSlice();
                } else {
                    // This slice is the last one
                    memcpy(b + offset, buffer + upto, len * sizeof(uint8_t));
                    upto += len;
                    break;
                }
            }
        }
        int64_t getFilePointer() const override { _CLTHROW_NOT_IMPLEMENT }
        int64_t length() const override { _CLTHROW_NOT_IMPLEMENT }
        void seek(const int64_t pos) override { _CLTHROW_NOT_IMPLEMENT }
        void close() override {}

        IndexInput *clone() const override { _CLTHROW_NOT_IMPLEMENT }
        const char *getDirectoryType() const override {
            return "";
        }
        const char *getObjectName() const override {
            return getClassName();
        }
        static const char *getClassName() {
            return "DocumentsWriter::ByteSliceReader";
        }

        friend class FieldMergeState;
    };


    class FieldMergeState {
    private:
        typename ThreadState::FieldData *field;
        CL_NS(util)::ValueArray<Posting *> *postings;

        Posting *p;
        T *text;
        int32_t textOffset;

        int32_t postingUpto;

        ByteSliceReader freq;
        ByteSliceReader prox;

        int32_t docID;
        int32_t termFreq;

    public:
        FieldMergeState() {
            field = NULL;
            postings = NULL;
            p = NULL;
            text = NULL;
            textOffset = 0;
            postingUpto = -1;
            docID = 0;
            termFreq = 0;
        }
        ~FieldMergeState() = default;
        bool nextTerm() {
            postingUpto++;
            if (postingUpto == field->numPostings)
                return false;

            p = (*postings)[postingUpto];
            docID = 0;

            text = field->threadState->scharPool->buffers[p->textStart >> CHAR_BLOCK_SHIFT];
            textOffset = p->textStart & CHAR_BLOCK_MASK;

            if (p->freqUpto > p->freqStart)
                freq.init(field->threadState->postingsPool, p->freqStart, p->freqUpto);
            else
                freq.bufferOffset = freq.upto = freq.endIndex = 0;

            if (field->fieldInfo->hasProx) {
                prox.init(field->threadState->postingsPool, p->proxStart, p->proxUpto);
            }

            // Should always be true
            bool result = nextDoc();
            assert(result);

            return true;
        }

        bool nextDoc() {
            if (freq.bufferOffset + freq.upto == freq.endIndex) {
                if (p->lastDocCode != -1) {
                    // Return last doc
                    docID = p->lastDocID;

                    if (field->fieldInfo->hasProx) {
                        termFreq = p->docFreq;
                    } else {
                        termFreq = 1;
                    }
                    
                    p->lastDocCode = -1;
                    return true;
                } else
                    // EOF
                    return false;
            }

            const auto code = (uint32_t) freq.readVInt();
            docID += code >> 1;//unsigned shift
            if ((code & 1) != 0)
                termFreq = 1;
            else
                termFreq = freq.readVInt();

            return true;
        }

        friend class SDocumentsWriter<T>;
    };


public:
    // Holds free pool of Posting instances
    CL_NS(util)::ObjectArray<Posting> postingsFreeListDW;
    int32_t postingsFreeCountDW;
    int32_t postingsAllocCountDW;
    typedef CL_NS(util)::CLArrayList<T *, CL_NS(util)::Deletor::vArray<T>> FreeSCharBlocksType;
    FreeSCharBlocksType freeSCharBlocks;
    typedef CL_NS(util)::CLArrayList<uint8_t *, CL_NS(util)::Deletor::vArray<uint8_t>> FreeByteBlocksType;
    FreeByteBlocksType freeByteBlocks;
    int64_t numBytesAlloc;
    int64_t numBytesUsed;
    std::vector<std::string> newFiles;
    std::string docStoreSegment;// Current doc-store segment we are writing
    int32_t docStoreOffset;     // Current starting doc-store offset of current segment

public:
    SDocumentsWriter(CL_NS(store)::Directory *directory, IndexWriter *writer) : freeSCharBlocks(FreeSCharBlocksType(true)),
                                                                                freeByteBlocks(FreeByteBlocksType(true)) {
        numBytesAlloc = 0;
        numBytesUsed = 0;
        this->directory = directory;
        this->writer = writer;
        this->hasNorms = this->bufferIsFull = false;

        this->closed = this->flushPending = false;
        this->threadState = nullptr;
        this->infoStream = nullptr;
        fieldInfos = _CLNEW FieldInfos();

        this->closed = this->flushPending = false;
        postingsFreeCountDW = postingsAllocCountDW = 0;
        docStoreOffset = nextDocID = numDocsInRAM = numDocsInStore = nextWriteDocID = 0;
        ramBufferSize = (int64_t) (256 * 1024 * 1024);
        maxBufferedDocs = IndexWriter::DEFAULT_MAX_BUFFERED_DOCS;
    }
    virtual ~SDocumentsWriter();
    int32_t flush(bool closeDocStore) override ;
    bool addDocument(CL_NS(document)::Document *doc, CL_NS(analysis)::Analyzer *sanalyzer) override {
        return updateDocument(doc, sanalyzer);
    }
    bool addNullDocument(CL_NS(document)::Document *doc) override {
        return updateNullDocument(doc);
    }
    bool updateDocument(CL_NS(document)::Document *doc, CL_NS(analysis)::Analyzer *sanalyzer);
    bool updateNullDocument(CL_NS(document)::Document *doc);
    ThreadState *getThreadState(CL_NS(document)::Document *doc);
    T *getSCharBlock();
    void recycleSCharBlocks(CL_NS(util)::ArrayBase<T *> &blocks, int32_t start, int32_t numBlocks);
    uint8_t *getByteBlock(bool trackAllocations);
    void recycleBlocks(CL_NS(util)::ArrayBase<uint8_t *> &blocks, int32_t start, int32_t end);
    void finishDocument(ThreadState *state);
    void getPostings(CL_NS(util)::ValueArray<Posting *> &postings);
    static void fillBytes(CL_NS(store)::IndexOutput *out, uint8_t b, int32_t numBytes);
    void appendPostings(CL_NS(util)::ArrayBase<typename ThreadState::FieldData *> *fields,
                        STermInfosWriter<T> *termsOut,
                        CL_NS(store)::IndexOutput *freqOut,
                        CL_NS(store)::IndexOutput *proxOut);

    void writeSegment(std::vector<std::string> &flushedFiles);

    void recyclePostings(CL_NS(util)::ValueArray<Posting *> &postings, int32_t numPostings);
    void writeNorms(const std::string &segmentName, int32_t totalNumDoc);
    int32_t compareText(const T *text1, const T *text2);
    void resetPostingsData() {
        // All ThreadStates should be idle when we are called
        segment.erase();
        numDocsInRAM = 0;
        nextDocID = 0;
        nextWriteDocID = 0;
        balanceRAM();
        bufferIsFull = false;
        flushPending = false;
        threadState->numThreads = 0;
        threadState->resetPostings();
        numBytesUsed = 0;
    }


    std::string segmentFileName(const std::string &extension) {
        return segment + "." + extension;
    }
    std::string segmentFileName(const char *extension) {
        return segmentFileName(string(extension));
    }
    int32_t getMaxBufferedDocs() override {
        return maxBufferedDocs;
    }

    int32_t getNumDocsInRAM() override {
        return numDocsInRAM;
    }
    const std::string &getDocStoreSegment() override { return docStoreSegment; }
    bool hasDeletes() override { return false; }
    std::string getSegment() override {
        return segment;
    }
    int32_t getDocStoreOffset() override {
        return docStoreOffset;
    }
    int32_t getNumBufferedDeleteTerms() override {
        return 0;
    }
    void abort(AbortException *ae) override {}
    void setMaxBufferedDocs(int32_t count) override {
        maxBufferedDocs = count;
    }
    void setInfoStream(std::ostream *is) override {
        this->infoStream = is;
    }
    void setRAMBufferSizeMB(float_t mb) override {
        if ((int32_t) mb == -1) {
            ramBufferSize = -1;
        } else {
            ramBufferSize = (int64_t) (mb * 1024 * 1024);
        }
    }

    float_t getRAMBufferSizeMB() override {
        if (ramBufferSize == -1) {
            return (float_t) ramBufferSize;
        } else {
            return ramBufferSize / 1024.0 / 1024.0;
        }
    }

    void close() override {}
    const std::vector<std::string>& files() override {
        if (_files != nullptr)
            return *_files;
        _files = _CLNEW std::vector<string>;
        return *_files;
    }
    std::string toMB(int64_t v) {
        char buf[40];
        cl_sprintf(buf, 40, "%0.2f", v / 1024.0 / 1024.0);
        return string(buf);
    }
    void balanceRAM();
    void setMaxBufferedDeleteTerms(int32_t _maxBufferedDeleteTerms) override {_CLTHROW_NOT_IMPLEMENT}
    int32_t getMaxBufferedDeleteTerms() override {_CLTHROW_NOT_IMPLEMENT}
    std::string closeDocStore() override {_CLTHROW_NOT_IMPLEMENT}
    const std::vector<string> *abortedFiles() override {_CLTHROW_NOT_IMPLEMENT}
    bool bufferDeleteTerm(Term *term) override {_CLTHROW_NOT_IMPLEMENT}
    bool pauseAllThreads() override {_CLTHROW_NOT_IMPLEMENT}
    void resumeAllThreads() override {_CLTHROW_NOT_IMPLEMENT}
    void createCompoundFile(const std::string &s) override {_CLTHROW_NOT_IMPLEMENT}
    void clearBufferedDeletes() override {_CLTHROW_NOT_IMPLEMENT}
    const TermNumMapType &getBufferedDeleteTerms() override {_CLTHROW_NOT_IMPLEMENT}
    bool updateDocument(Term *t, CL_NS(document)::Document *doc, CL_NS(analysis)::Analyzer *analyzer) override {_CLTHROW_NOT_IMPLEMENT}
    bool bufferDeleteTerms(const CL_NS(util)::ArrayBase<Term *> *terms) override {_CLTHROW_NOT_IMPLEMENT}
    int64_t getRAMUsed() override {_CLTHROW_NOT_IMPLEMENT}
    const std::vector<int32_t> *getBufferedDeleteDocIDs() override {_CLTHROW_NOT_IMPLEMENT}

    bool hasProx() override;

private:
    std::vector<std::string>* _files = nullptr;

public:
    ThreadState *threadState;
    CL_NS(util)::ObjectArray<BufferedNorms> norms;// Holds norms until we flush
    DefaultSkipListWriter *skipListWriter{};
    TermInfo termInfo;// minimize consing

    static const int32_t nextLevelArray[10];
    static const int32_t levelSizeArray[10];
    static const int32_t BYTE_BLOCK_SHIFT;
    static const int32_t BYTE_BLOCK_SIZE;
    static const int32_t BYTE_BLOCK_MASK;
    static const int32_t BYTE_BLOCK_NOT_MASK;

    static const int32_t CHAR_BLOCK_SHIFT;
    static const int32_t CHAR_BLOCK_SIZE;
    static const int32_t CHAR_BLOCK_MASK;

    static const int32_t POINTER_NUM_BYTE;
    static const int32_t INT_NUM_BYTE;
    static const int32_t CHAR_NUM_BYTE;
    static const int32_t SCHAR_NUM_BYTE;

    static const int32_t POSTING_NUM_BYTE;/// = OBJECT_HEADER_BYTES + 9*INT_NUM_BYTE + 5*POINTER_NUM_BYTE;
    static int32_t OBJECT_HEADER_BYTES;

    static const uint8_t defaultNorm;///=Similarity::encodeNorm(1.0f)
};
#define CLUCENE_END_OF_WORD 0x0

CL_NS_END
#endif
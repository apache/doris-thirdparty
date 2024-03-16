/*------------------------------------------------------------------------------
* Copyright (C) 2003-2006 Ben van Klinken and the CLucene Team
*
* Distributable under the terms of either the Apache License (Version 2.0) or
* the GNU Lesser General Public License, as specified in the COPYING file.
------------------------------------------------------------------------------*/
#include "CLucene/_ApiHeader.h"

#include "CLucene/analysis/AnalysisHeader.h"
#include "CLucene/analysis/Analyzers.h"
#include "CLucene/document/Document.h"
#include "CLucene/search/Similarity.h"
#include "CLucene/store/Directory.h"
#include "CLucene/util/Misc.h"
#include "CLucene/util/PFORUtil.h"
#include "IndexReader.h"
#include "IndexWriter.h"

#include "CLucene/index/MergePolicy.h"
#include "CLucene/search/Similarity.h"
#include "CLucene/store/FSDirectory.h"
#include "CLucene/store/_Lock.h"
#include "CLucene/store/_RAMDirectory.h"
#include "CLucene/util/Array.h"
#include "CLucene/util/PriorityQueue.h"
#include "CLucene/index/CodeMode.h"
#include "CLucene/analysis/standard95/StandardAnalyzer.h"
#include "MergePolicy.h"
#include "MergeScheduler.h"
#include "SDocumentWriter.h"
#include "_DocumentsWriter.h"
#include "_IndexFileDeleter.h"
#include "_SegmentHeader.h"
#include "_SegmentInfos.h"
#include "_SegmentMerger.h"
#include "_SkipListWriter.h"
#include "_Term.h"
#include "_TermInfo.h"
#include <algorithm>
#include <memory>
#include <assert.h>
#include <iostream>
#include <roaring/roaring.hh>
#include <sstream>

#define FINALLY_CLOSE_OUTPUT(x)       \
    try {                             \
        if (x != nullptr) x->close(); \
    } catch (...) {                   \
    }

CL_NS_USE(store)
CL_NS_USE(util)
CL_NS_USE(document)
CL_NS_USE(analysis)
CL_NS_USE(search)
CL_NS_DEF(index)

int64_t IndexWriter::WRITE_LOCK_TIMEOUT = 1000;
const char *IndexWriter::WRITE_LOCK_NAME = "write.lock";
const char *IndexWriter::NULL_BITMAP_FILE_NAME = "null_bitmap";
std::ostream *IndexWriter::defaultInfoStream = NULL;

const int32_t IndexWriter::MERGE_READ_BUFFER_SIZE = 4096;
const int32_t IndexWriter::DISABLE_AUTO_FLUSH = -1;
const int32_t IndexWriter::DEFAULT_MAX_BUFFERED_DOCS = DISABLE_AUTO_FLUSH;
const float_t IndexWriter::DEFAULT_RAM_BUFFER_SIZE_MB = 16.0;
const int32_t IndexWriter::DEFAULT_MAX_BUFFERED_DELETE_TERMS = DISABLE_AUTO_FLUSH;
const int32_t IndexWriter::DEFAULT_MAX_MERGE_DOCS = LogDocMergePolicy::DEFAULT_MAX_MERGE_DOCS;
const int32_t IndexWriter::DEFAULT_MERGE_FACTOR = LogMergePolicy::DEFAULT_MERGE_FACTOR;

DEFINE_MUTEX(IndexWriter::MESSAGE_ID_LOCK)
int32_t IndexWriter::MESSAGE_ID = 0;
const int32_t IndexWriter::MAX_TERM_LENGTH = DocumentsWriter::MAX_TERM_LENGTH;

class IndexWriter::Internal {
public:
    IndexWriter *_this;
    Internal(IndexWriter *_this) {
        this->_this = _this;
    }
    // Apply buffered delete terms to the segment just flushed from ram
    // apply appropriately so that a delete term is only applied to
    // the documents buffered before it, not those buffered after it.
    void applyDeletesSelectively(const TermNumMapType &deleteTerms,
                                 const std::vector<int32_t> &deleteIds, IndexReader *reader);

    // Apply buffered delete terms to this reader.
    void applyDeletes(const TermNumMapType &deleteTerms, IndexReader *reader);
};

void IndexWriter::deinit(bool releaseWriteLock) throw() {
    if (writeLock != NULL && releaseWriteLock) {
        writeLock->release();// release write lock
        _CLLDELETE(writeLock);
    }
    _CLLDELETE(segmentInfos);
    _CLLDELETE(mergingSegments);
    _CLLDELETE(pendingMerges);
    _CLLDELETE(runningMerges);
    _CLLDELETE(mergeExceptions);
    _CLLDELETE(segmentsToOptimize);
    _CLLDELETE(mergeScheduler);
    _CLLDELETE(mergePolicy);
    _CLLDELETE(deleter);
    _CLLDELETE(docWriter);
    if (bOwnsDirectory) _CLLDECDELETE(directory);
    delete _internal;
}

IndexWriter::~IndexWriter() {
    deinit();

    _trans_vec.clear();
    readers.clear();
    if (fieldInfos != nullptr) {
        _CLDELETE(fieldInfos);
    }
    freqOutputList.clear();
    proxOutputList.clear();
    termInfosWriterList.clear();
    skipListWriterList.clear();
    docDeltaBuffer.clear();
}

void IndexWriter::ensureOpen() {
    if (closed) {
        _CLTHROWA(CL_ERR_AlreadyClosed, "this IndexWriter is closed");
    }
}

void IndexWriter::message(string message) {
    if (infoStream != NULL) {
        (*infoStream) << string("IW ") << Misc::toString(messageID) << string(" [")
                      << Misc::toString(_LUCENE_CURRTHREADID) << string("]: ") << message << string("\n");
    }
}

void IndexWriter::setMessageID() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    if (infoStream != NULL && messageID == -1) {
        {
            SCOPED_LOCK_MUTEX(MESSAGE_ID_LOCK)
            messageID = MESSAGE_ID++;
        }
    }
}

LogMergePolicy *IndexWriter::getLogMergePolicy() const {
    if (mergePolicy->instanceOf(LogMergePolicy::getClassName()))
        return (LogMergePolicy *) mergePolicy;
    else
        _CLTHROWA(CL_ERR_IllegalArgument, "this method can only be called when the merge policy is the default LogMergePolicy");
}

bool IndexWriter::getUseCompoundFile() {
    return getLogMergePolicy()->getUseCompoundFile();
}


void IndexWriter::setUseCompoundFile(bool value) {
    getLogMergePolicy()->setUseCompoundFile(value);
    getLogMergePolicy()->setUseCompoundDocStore(value);
}

void IndexWriter::setSimilarity(Similarity *similarity) {
    ensureOpen();
    this->similarity = similarity;
}

Similarity *IndexWriter::getSimilarity() {
    ensureOpen();
    return this->similarity;
}


void IndexWriter::setTermIndexInterval(int32_t interval) {
    ensureOpen();
    this->termIndexInterval = interval;
}

int32_t IndexWriter::getTermIndexInterval() {
    ensureOpen();
    return termIndexInterval;
}

IndexWriter::IndexWriter(const char *path, Analyzer *a, bool create) : bOwnsDirectory(true) {
    init(FSDirectory::getDirectory(path, create), a, create, true, (IndexDeletionPolicy *) NULL, true);
}

IndexWriter::IndexWriter(Directory *d, Analyzer *a, bool create, bool closeDir) : bOwnsDirectory(false) {
    init(d, a, create, closeDir, NULL, true);
}

IndexWriter::IndexWriter(Directory *d, bool autoCommit, Analyzer *a, IndexDeletionPolicy *deletionPolicy, bool closeDirOnShutdown) : bOwnsDirectory(false) {
    init(d, a, closeDirOnShutdown, deletionPolicy, autoCommit);
}

IndexWriter::IndexWriter(Directory *d, bool autoCommit, Analyzer *a, bool create, IndexDeletionPolicy *deletionPolicy, bool closeDirOnShutdown) : bOwnsDirectory(false) {
    init(d, a, create, closeDirOnShutdown, deletionPolicy, autoCommit);
}

void IndexWriter::init(Directory *d, Analyzer *a, bool closeDir, IndexDeletionPolicy *deletionPolicy, bool autoCommit) {
    if (IndexReader::indexExists(d)) {
        init(d, a, false, closeDir, deletionPolicy, autoCommit);
    } else {
        init(d, a, true, closeDir, deletionPolicy, autoCommit);
    }
}

void IndexWriter::init(Directory *d, Analyzer *a, const bool create, const bool closeDir,
                       IndexDeletionPolicy *deletionPolicy, const bool autoCommit) {
    this->_internal = new Internal(this);
    this->termIndexInterval = IndexWriter::DEFAULT_TERM_INDEX_INTERVAL;
    this->mergeScheduler = _CLNEW SerialMergeScheduler();//TODO: implement and use ConcurrentMergeScheduler
    this->mergingSegments = _CLNEW MergingSegmentsType;
    this->pendingMerges = _CLNEW PendingMergesType;
    this->runningMerges = _CLNEW RunningMergesType;
    this->mergeExceptions = _CLNEW MergeExceptionsType;
    this->segmentsToOptimize = _CLNEW SegmentsToOptimizeType;
    this->mergePolicy = _CLNEW LogByteSizeMergePolicy();
    this->localRollbackSegmentInfos = NULL;
    this->stopMerges = false;
    messageID = -1;
    maxFieldLength = FIELD_TRUNC_POLICY__WARN;
    infoStream = NULL;
    this->mergeFactor = this->minMergeDocs = this->maxMergeDocs = 0;
    this->commitLockTimeout = 0;
    this->closeDir = closeDir;
    this->commitPending = this->closed = this->closing = false;
    directory = d;
    analyzer = a;
    this->infoStream = defaultInfoStream;
    setMessageID();
    this->writeLockTimeout = IndexWriter::WRITE_LOCK_TIMEOUT;
    this->similarity = Similarity::getDefault();
    this->hitOOM = false;
    this->autoCommit = true;
    this->segmentInfos = _CLNEW SegmentInfos;
    this->mergeGen = 0;
    this->rollbackSegmentInfos = NULL;
    this->deleter = NULL;
    this->docWriter = NULL;
    this->writeLock = NULL;
    this->fieldInfos = NULL;

    if (create) {
        // Clear the write lock in case it's leftover:
        directory->clearLock(IndexWriter::WRITE_LOCK_NAME);
    }

    bool hasLock = false;
    try {
        writeLock = directory->makeLock(IndexWriter::WRITE_LOCK_NAME);
        hasLock = writeLock->obtain(writeLockTimeout);
        if (!hasLock)// obtain write lock
            _CLTHROWA(CL_ERR_LockObtainFailed, (string("Index locked for write: ") + writeLock->getObjectName()).c_str());
    } catch (...) {
        deinit(hasLock);
        throw;
    }

    try {
        if (create) {
            // Try to read first.  This is to allow create
            // against an index that's currently open for
            // searching.  In this case we write the next
            // segments_N file with no segments:
            //NOTE: do not read when create, because doris would never read an old index dir
            /*try {
                segmentInfos->read(directory);
                segmentInfos->clear();
            } catch (CLuceneError &e) {
                if (e.number() != CL_ERR_IO) throw e;
                // Likely this means it's a fresh directory
            }*/
            segmentInfos->write(directory);
        } else {
            segmentInfos->read(directory);
        }

        this->autoCommit = autoCommit;
        if (!autoCommit) {
            rollbackSegmentInfos = segmentInfos->clone();
        } else {
            rollbackSegmentInfos = NULL;
        }
        if (analyzer != nullptr) {
            if (analyzer->isSDocOpt()) {
                docWriter = _CLNEW SDocumentsWriter<char>(directory, this);
            } else {
                _CLTHROWA(CL_ERR_IllegalArgument, "IndexWriter::init: Only support SDocumentsWriter");
            }
        } else {
            _CLTHROWA(CL_ERR_IllegalArgument, "IndexWriter::init: Only support SDocumentsWriter");
        }
        // Default deleter (for backwards compatibility) is
        // KeepOnlyLastCommitDeleter:
        deleter = _CLNEW IndexFileDeleter(directory,
                                          deletionPolicy == NULL ? _CLNEW KeepOnlyLastCommitDeletionPolicy() : deletionPolicy,
                                          segmentInfos, infoStream, docWriter);

        pushMaxBufferedDocs();

        if (infoStream != NULL) {
            message(string("init: create=") + (create ? "true" : "false"));
            messageState();
        }

    } catch (CLuceneError &e) {
        deinit(e.number() == CL_ERR_IO);
        throw e;
    }
}

void IndexWriter::setMergePolicy(MergePolicy *mp) {
    ensureOpen();
    if (mp == NULL)
        _CLTHROWA(CL_ERR_NullPointer, "MergePolicy must be non-NULL");

    if (mergePolicy != mp) {
        mergePolicy->close();
        _CLDELETE(mergePolicy);
    }
    mergePolicy = mp;
    pushMaxBufferedDocs();
    if (infoStream != NULL)
        message(string("setMergePolicy ") + mp->getObjectName());
}

MergePolicy *IndexWriter::getMergePolicy() {
    ensureOpen();
    return mergePolicy;
}

void IndexWriter::setMergeScheduler(MergeScheduler *mergeScheduler) {
    ensureOpen();
    if (mergeScheduler == NULL)
        _CLTHROWA(CL_ERR_NullPointer, "MergeScheduler must be non-NULL");

    if (this->mergeScheduler != mergeScheduler) {
        finishMerges(true);
        this->mergeScheduler->close();
        _CLLDELETE(this->mergeScheduler)
    }
    this->mergeScheduler = mergeScheduler;
    if (infoStream != NULL)
        message(string("setMergeScheduler ") + mergeScheduler->getObjectName());
}

MergeScheduler *IndexWriter::getMergeScheduler() {
    ensureOpen();
    return mergeScheduler;
}

void IndexWriter::setMaxMergeDocs(int32_t maxMergeDocs) {
    getLogMergePolicy()->setMaxMergeDocs(maxMergeDocs);
}

int32_t IndexWriter::getMaxMergeDocs() const {
    return getLogMergePolicy()->getMaxMergeDocs();
}

void IndexWriter::setMaxFieldLength(int32_t maxFieldLength) {
    ensureOpen();
    this->maxFieldLength = maxFieldLength;
    if (infoStream != NULL)
        message("setMaxFieldLength " + Misc::toString(maxFieldLength));
}

int32_t IndexWriter::getMaxFieldLength() {
    ensureOpen();
    return maxFieldLength;
}

void IndexWriter::setMaxBufferedDocs(int32_t maxBufferedDocs) {
    ensureOpen();
    if (maxBufferedDocs != DISABLE_AUTO_FLUSH && maxBufferedDocs < 2)
        _CLTHROWA(CL_ERR_IllegalArgument,
                  "maxBufferedDocs must at least be 2 when enabled");
    if (maxBufferedDocs == DISABLE_AUTO_FLUSH && (int32_t) getRAMBufferSizeMB() == DISABLE_AUTO_FLUSH)
        _CLTHROWA(CL_ERR_IllegalArgument,
                  "at least one of ramBufferSize and maxBufferedDocs must be enabled");
    docWriter->setMaxBufferedDocs(maxBufferedDocs);
    pushMaxBufferedDocs();
    if (infoStream != NULL)
        message("setMaxBufferedDocs " + Misc::toString(maxBufferedDocs));
}

void IndexWriter::pushMaxBufferedDocs() {
    if (docWriter->getMaxBufferedDocs() != DISABLE_AUTO_FLUSH) {
        const MergePolicy *mp = mergePolicy;
        if (mp->instanceOf(LogDocMergePolicy::getClassName())) {
            LogDocMergePolicy *lmp = (LogDocMergePolicy *) mp;
            const int32_t maxBufferedDocs = docWriter->getMaxBufferedDocs();
            if (lmp->getMinMergeDocs() != maxBufferedDocs) {
                if (infoStream != NULL) {
                    message(string("now push maxBufferedDocs ") + Misc::toString(maxBufferedDocs) + " to LogDocMergePolicy");
                }
                lmp->setMinMergeDocs(maxBufferedDocs);
            }
        }
    }
}

int32_t IndexWriter::getMaxBufferedDocs() {
    ensureOpen();
    return docWriter->getMaxBufferedDocs();
}

void IndexWriter::setRAMBufferSizeMB(float_t mb) {
    if ((int32_t) mb != DISABLE_AUTO_FLUSH && mb <= 0.0)
        _CLTHROWA(CL_ERR_IllegalArgument,
                  "ramBufferSize should be > 0.0 MB when enabled");
    if (mb == DISABLE_AUTO_FLUSH && getMaxBufferedDocs() == DISABLE_AUTO_FLUSH)
        _CLTHROWA(CL_ERR_IllegalArgument,
                  "at least one of ramBufferSize and maxBufferedDocs must be enabled");
    docWriter->setRAMBufferSizeMB(mb);
    if (infoStream != NULL) {
        message(string("setRAMBufferSizeMB ") + Misc::toString(mb));
    }
}

float_t IndexWriter::getRAMBufferSizeMB() {
    return docWriter->getRAMBufferSizeMB();
}

void IndexWriter::setMaxBufferedDeleteTerms(int32_t maxBufferedDeleteTerms) {
    ensureOpen();
    if (maxBufferedDeleteTerms != DISABLE_AUTO_FLUSH && maxBufferedDeleteTerms < 1)
        _CLTHROWA(CL_ERR_IllegalArgument,
                  "maxBufferedDeleteTerms must at least be 1 when enabled");
    docWriter->setMaxBufferedDeleteTerms(maxBufferedDeleteTerms);
    if (infoStream != NULL)
        message("setMaxBufferedDeleteTerms " + Misc::toString(maxBufferedDeleteTerms));
}

int32_t IndexWriter::getMaxBufferedDeleteTerms() {
    ensureOpen();
    return docWriter->getMaxBufferedDeleteTerms();
}

void IndexWriter::setMergeFactor(int32_t mergeFactor) {
    getLogMergePolicy()->setMergeFactor(mergeFactor);
}

int32_t IndexWriter::getMergeFactor() const {
    return getLogMergePolicy()->getMergeFactor();
}

void IndexWriter::setDefaultInfoStream(std::ostream *infoStream) {
    IndexWriter::defaultInfoStream = infoStream;
}

std::ostream *IndexWriter::getDefaultInfoStream() {
    return IndexWriter::defaultInfoStream;
}

//TODO: infoStream - unicode
void IndexWriter::setInfoStream(std::ostream *infoStream) {
    ensureOpen();
    this->infoStream = infoStream;
    setMessageID();
    docWriter->setInfoStream(infoStream);
    deleter->setInfoStream(infoStream);
    if (infoStream != NULL)
        messageState();
}

void IndexWriter::messageState() {
    message(string("setInfoStream: dir=") + directory->toString() +
            " autoCommit=" + (autoCommit ? "true" : "false" + string(" mergePolicy=") + mergePolicy->getObjectName() + " mergeScheduler=" + mergeScheduler->getObjectName() + " ramBufferSizeMB=" + Misc::toString(docWriter->getRAMBufferSizeMB()) + " maxBuffereDocs=" + Misc::toString(docWriter->getMaxBufferedDocs())) +
            " maxFieldLength=" + Misc::toString(maxFieldLength) +
            " index=" + segString());
}

std::ostream *IndexWriter::getInfoStream() {
    ensureOpen();
    return infoStream;
}

void IndexWriter::setWriteLockTimeout(int64_t writeLockTimeout) {
    ensureOpen();
    this->writeLockTimeout = writeLockTimeout;
}

int64_t IndexWriter::getWriteLockTimeout() {
    ensureOpen();
    return writeLockTimeout;
}

void IndexWriter::setDefaultWriteLockTimeout(int64_t writeLockTimeout) {
    IndexWriter::WRITE_LOCK_TIMEOUT = writeLockTimeout;
}

int64_t IndexWriter::getDefaultWriteLockTimeout() {
    return IndexWriter::WRITE_LOCK_TIMEOUT;
}

void IndexWriter::close(bool waitForMerges) {
    bool doClose;

    // If any methods have hit OutOfMemoryError, then abort
    // on close, in case the internal state of IndexWriter
    // or DocumentsWriter is corrupt
    if (hitOOM)
        abort();

    {
        SCOPED_LOCK_MUTEX(this->THIS_LOCK)
        // Ensure that only one thread actually gets to do the closing:
        if (!closing) {
            doClose = true;
            closing = true;
        } else
            doClose = false;
    }
    if (doClose)
        closeInternal(waitForMerges);
    else
        // Another thread beat us to it (is actually doing the
        // close), so we will block until that other thread
        // has finished closing
        waitForClose();
}

void IndexWriter::waitForClose() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    while (!closed && closing) {
        CONDITION_WAIT(THIS_LOCK, THIS_WAIT_CONDITION)
    }
}

void IndexWriter::closeInternal(bool waitForMerges) {
    try {
        if (infoStream != NULL)
            message(string("now flush at close"));

        docWriter->close();

        // Only allow a _CLNEW merge to be triggered if we are
        // going to wait for merges:
        flush(waitForMerges, true);

        if (waitForMerges)
            // Give merge scheduler last chance to run, in case
            // any pending merges are waiting:
            mergeScheduler->merge(this);

        mergePolicy->close();

        finishMerges(waitForMerges);

        mergeScheduler->close();

        {
            SCOPED_LOCK_MUTEX(this->THIS_LOCK)
            if (commitPending) {
                bool success = false;
                try {
                    segmentInfos->write(directory);// now commit changes
                    success = true;
                }
                _CLFINALLY(
                        if (!success) {
                            if (infoStream != NULL)
                                message(string("hit exception committing segments file during close"));
                            deletePartialSegmentsFile();
                        })
                if (infoStream != NULL)
                    message("close: wrote segments file \"" + segmentInfos->getCurrentSegmentFileName() + "\"");

                deleter->checkpoint(segmentInfos, true);

                commitPending = false;
                //        _CLDELETE(rollbackSegmentInfos);
            }
            _CLDELETE(rollbackSegmentInfos);


            if (infoStream != NULL)
                message("at close: " + segString());

            _CLDELETE(docWriter);
            deleter->close();
        }

        if (closeDir)
            directory->close();

        if (writeLock != NULL) {
            writeLock->release();// release write lock
            _CLDELETE(writeLock);
        }
        closed = true;
    } catch (std::bad_alloc &) {
        hitOOM = true;
        _CLTHROWA(CL_ERR_OutOfMemory, "Out of memory");
    } catch (CLuceneError &e) {
        throw e;
    }
    _CLFINALLY(
            {
                SCOPED_LOCK_MUTEX(this->THIS_LOCK)
                if (!closed) {
                    closing = false;
                    if (infoStream != NULL)
                        message(string("hit exception while closing"));
                }
                CONDITION_NOTIFYALL(THIS_WAIT_CONDITION)
            })
}

bool IndexWriter::flushDocStores() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)

    const std::vector<std::string> &files = docWriter->files();

    bool useCompoundDocStore = false;

    if (files.size() > 0) {
        string docStoreSegment;

        bool success = false;
        try {
            docStoreSegment = docWriter->closeDocStore();
            success = true;
        }
        _CLFINALLY(
                if (!success) {
                    if (infoStream != NULL)
                        message(string("hit exception closing doc store segment"));
                    docWriter->abort(NULL);
                })

        useCompoundDocStore = mergePolicy->useCompoundDocStore(segmentInfos);

        if (useCompoundDocStore && !docStoreSegment.empty()) {
            // Now build compound doc store file

            success = false;

            const int32_t numSegments = segmentInfos->size();
            const string compoundFileName = docStoreSegment + "." + IndexFileNames::COMPOUND_FILE_STORE_EXTENSION;

            try {
                CompoundFileWriter cfsWriter(directory, compoundFileName.c_str());
                const size_t size = files.size();
                for (size_t i = 0; i < size; ++i)
                    cfsWriter.addFile(files[i].c_str());

                // Perform the merge
                cfsWriter.close();

                for (int32_t i = 0; i < numSegments; i++) {
                    SegmentInfo *si = segmentInfos->info(i);
                    if (si->getDocStoreOffset() != -1 &&
                        si->getDocStoreSegment().compare(docStoreSegment) == 0)
                        si->setDocStoreIsCompoundFile(true);
                }
                checkpoint();
                success = true;
            }
            _CLFINALLY(
                    if (!success) {
                        if (infoStream != NULL)
                            message("hit exception building compound file doc store for segment " + docStoreSegment);

                        // Rollback to no compound file
                        for (int32_t i = 0; i < numSegments; i++) {
                            SegmentInfo *si = segmentInfos->info(i);
                            if (si->getDocStoreOffset() != -1 &&
                                si->getDocStoreSegment().compare(docStoreSegment) == 0)
                                si->setDocStoreIsCompoundFile(false);
                        }
                        deleter->deleteFile(compoundFileName.c_str());
                        deletePartialSegmentsFile();
                    })

            deleter->checkpoint(segmentInfos, false);
        }
    }

    return useCompoundDocStore;
}

Directory *IndexWriter::getDirectory() {
    ensureOpen();
    return directory;
}

Analyzer *IndexWriter::getAnalyzer() {
    ensureOpen();
    return analyzer;
}

int32_t IndexWriter::docCount() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    ensureOpen();
    int32_t count = docWriter->getNumDocsInRAM();
    for (int32_t i = 0; i < segmentInfos->size(); i++) {
        SegmentInfo *si = segmentInfos->info(i);
        count += si->docCount;
    }
    return count;
}

void IndexWriter::addDocument(Document *doc, Analyzer *an) {
    if (an == NULL) an = this->analyzer;
    ensureOpen();
    bool doFlush = false;
    bool success = false;
    try {
        try {
            doFlush = docWriter->addDocument(doc, an);
            success = true;
        }
        _CLFINALLY(
                if (!success) {
                    if (infoStream != NULL)
                        message(string("hit exception adding document"));

                    {
                        std::vector<std::string> files;
                        directory->list(files);
                        for (auto& file : files) {
                            directory->deleteFile(file.c_str());
                        }
                    }
                })
        if (doFlush)
            flush(true, false);
    } catch (std::bad_alloc &) {
        hitOOM = true;
        _CLTHROWA(CL_ERR_OutOfMemory, "Out of memory");
    }
}

void IndexWriter::addNullDocument(Document *doc) {
    ensureOpen();
    docWriter->addNullDocument(doc);
}

void IndexWriter::deleteDocuments(Term *term) {
    ensureOpen();
    try {
        bool doFlush = docWriter->bufferDeleteTerm(term);
        if (doFlush)
            flush(true, false);
    } catch (std::bad_alloc &) {
        hitOOM = true;
        _CLTHROWA(CL_ERR_OutOfMemory, "Out of memory");
    }
}

void IndexWriter::deleteDocuments(const ArrayBase<Term *> *terms) {
    ensureOpen();
    try {
        bool doFlush = docWriter->bufferDeleteTerms(terms);
        if (doFlush)
            flush(true, false);
    } catch (std::bad_alloc &) {
        hitOOM = true;
        _CLTHROWA(CL_ERR_OutOfMemory, "Out of memory");
    }
}

void IndexWriter::updateDocument(Term *term, Document *doc) {
    ensureOpen();
    updateDocument(term, doc, getAnalyzer());
}

void IndexWriter::updateDocument(Term *term, Document *doc, Analyzer *analyzer) {
    ensureOpen();
    try {
        bool doFlush = false;
        bool success = false;
        try {
            doFlush = docWriter->updateDocument(term, doc, analyzer);
            success = true;
        }
        _CLFINALLY(
                if (!success) {
                    if (infoStream != NULL)
                        message(string("hit exception updating document"));

                    {
                        SCOPED_LOCK_MUTEX(this->THIS_LOCK)
                        // If docWriter has some aborted files that were
                        // never incref'd, then we clean them up here
                        const std::vector<std::string> *files = docWriter->abortedFiles();
                        if (files != NULL)
                            deleter->deleteNewFiles(*files);
                    }
                })
        if (doFlush)
            flush(true, false);
    } catch (std::bad_alloc &) {
        hitOOM = true;
        _CLTHROWA(CL_ERR_OutOfMemory, "Out of memory");
    }
}

// for test purpose
int32_t IndexWriter::getSegmentCount() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    return segmentInfos->size();
}

// for test purpose
int32_t IndexWriter::getNumBufferedDocuments() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    return docWriter->getNumDocsInRAM();
}

// for test purpose
int32_t IndexWriter::getDocCount(int32_t i) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    if (i >= 0 && i < segmentInfos->size()) {
        return segmentInfos->info(i)->docCount;
    } else {
        return -1;
    }
}

string IndexWriter::newSegmentName() {
    // Cannot synchronize on IndexWriter because that causes
    // deadlock
    {
        SCOPED_LOCK_MUTEX(segmentInfos->THIS_LOCK)
        // Important to set commitPending so that the
        // segmentInfos is written on close.  Otherwise we
        // could close, re-open and re-return the same segment
        // name that was previously returned which can cause
        // problems at least with ConcurrentMergeScheduler.
        commitPending = true;

        char buf[10];
        Misc::longToBase(segmentInfos->counter++, 36, buf);
        return string("_") + buf;
    }
}

void IndexWriter::optimize(bool doWait) {
    optimize(1, doWait);
}

void IndexWriter::optimize(int32_t maxNumSegments, bool doWait) {
    ensureOpen();

    if (maxNumSegments < 1)
        _CLTHROWA(CL_ERR_IllegalArgument, "maxNumSegments must be >= 1; got " + maxNumSegments);

    if (infoStream != NULL)
        message("optimize: index now " + segString());

    flush();

    {
        SCOPED_LOCK_MUTEX(this->THIS_LOCK)
        resetMergeExceptions();
        segmentsToOptimize->clear();
        const int32_t numSegments = segmentInfos->size();
        for (int32_t i = 0; i < numSegments; i++)
            segmentsToOptimize->push_back(segmentInfos->info(i));

        // Now mark all pending & running merges as optimize
        // merge:
        PendingMergesType::iterator it = pendingMerges->begin();
        while (it != pendingMerges->end()) {
            MergePolicy::OneMerge *_merge = *it;
            _merge->optimize = true;
            _merge->maxNumSegmentsOptimize = maxNumSegments;

            it++;
        }

        RunningMergesType::iterator it2 = runningMerges->begin();
        while (it2 != runningMerges->end()) {
            MergePolicy::OneMerge *_merge = *it2;
            _merge->optimize = true;
            _merge->maxNumSegmentsOptimize = maxNumSegments;

            it2++;
        }
    }

    maybeMerge(maxNumSegments, true);

    if (doWait) {
        {
            SCOPED_LOCK_MUTEX(this->THIS_LOCK)
            while (optimizeMergesPending()) {
                CONDITION_WAIT(THIS_LOCK, THIS_WAIT_CONDITION);

                if (mergeExceptions->size() > 0) {
                    // Forward any exceptions in background merge
                    // threads to the current thread:
                    const int32_t size = mergeExceptions->size();
                    for (int32_t i = 0; i < size; i++) {
                        MergePolicy::OneMerge *_merge = (*mergeExceptions)[0];
                        if (_merge->optimize) {
                            CLuceneError tmp(_merge->getException());
                            CLuceneError err(tmp.number(),
                                             (string("background merge hit exception: ") + _merge->segString(directory) + ":" + tmp.what()).c_str(), false);
                            throw err;
                        }
                    }
                }
            }
        }
    }

    // NOTE: in the ConcurrentMergeScheduler case, when
    // doWait is false, we can return immediately while
    // background threads accomplish the optimization
}

bool IndexWriter::optimizeMergesPending() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    for (PendingMergesType::iterator it = pendingMerges->begin();
         it != pendingMerges->end(); it++) {
        if ((*it)->optimize)
            return true;

        it++;
    }

    for (RunningMergesType::iterator it = runningMerges->begin();
         it != runningMerges->end(); it++) {
        if ((*it)->optimize)
            return true;

        it++;
    }

    return false;
}

void IndexWriter::maybeMerge() {
    maybeMerge(false);
}

void IndexWriter::maybeMerge(bool optimize) {
    maybeMerge(1, optimize);
}

void IndexWriter::maybeMerge(int32_t maxNumSegmentsOptimize, bool optimize) {
    updatePendingMerges(maxNumSegmentsOptimize, optimize);
    mergeScheduler->merge(this);
}

void IndexWriter::updatePendingMerges(int32_t maxNumSegmentsOptimize, bool optimize) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    assert(!optimize || maxNumSegmentsOptimize > 0);

    if (stopMerges)
        return;

    MergePolicy::MergeSpecification *spec;
    if (optimize) {
        spec = mergePolicy->findMergesForOptimize(segmentInfos, this, maxNumSegmentsOptimize, *segmentsToOptimize);

        if (spec != NULL) {
            const int32_t numMerges = spec->merges->size();
            for (int32_t i = 0; i < numMerges; i++) {
                MergePolicy::OneMerge *_merge = (*spec->merges)[i];
                _merge->optimize = true;
                _merge->maxNumSegmentsOptimize = maxNumSegmentsOptimize;
            }
        }

    } else
        spec = mergePolicy->findMerges(segmentInfos, this);

    if (spec != NULL) {
        const int32_t numMerges = spec->merges->size();
        for (int32_t i = 0; i < numMerges; i++)
            registerMerge((*spec->merges)[i]);
    }
    _CLDELETE(spec);
}

MergePolicy::OneMerge *IndexWriter::getNextMerge() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    if (pendingMerges->size() == 0)
        return NULL;
    else {
        // Advance the merge from pending to running
        MergePolicy::OneMerge *_merge = *pendingMerges->begin();
        pendingMerges->pop_front();
        runningMerges->insert(_merge);
        return _merge;
    }
}


void IndexWriter::startTransaction() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)

    if (infoStream != NULL)
        message(string("now start transaction"));

    CND_PRECONDITION(docWriter->getNumBufferedDeleteTerms() == 0,
                     "calling startTransaction with buffered delete terms not supported");
    CND_PRECONDITION(docWriter->getNumDocsInRAM() == 0,
                     "calling startTransaction with buffered documents not supported");

    localRollbackSegmentInfos = segmentInfos->clone();
    localAutoCommit = autoCommit;

    if (localAutoCommit) {

        if (infoStream != NULL)
            message(string("flush at startTransaction"));

        flush();
        // Turn off auto-commit during our local transaction:
        autoCommit = false;
    } else
        // We must "protect" our files at this point from
        // deletion in case we need to rollback:
        deleter->incRef(segmentInfos, false);
}

void IndexWriter::rollbackTransaction() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)

    if (infoStream != NULL)
        message(string("now rollback transaction"));

    // First restore autoCommit in case we hit an exception below:
    autoCommit = localAutoCommit;

    // Keep the same segmentInfos instance but replace all
    // of its SegmentInfo instances.  This is so the next
    // attempt to commit using this instance of IndexWriter
    // will always write to a _CLNEW generation ("write once").
    segmentInfos->clear();
    segmentInfos->insert(localRollbackSegmentInfos, true);
    _CLDELETE(localRollbackSegmentInfos);

    // Ask deleter to locate unreferenced files we had
    // created & remove them:
    deleter->checkpoint(segmentInfos, false);

    if (!autoCommit)
        // Remove the incRef we did in startTransaction:
        deleter->decRef(segmentInfos);

    deleter->refresh();
    finishMerges(false);
    stopMerges = false;
}

void IndexWriter::commitTransaction() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)

    if (infoStream != NULL)
        message(string("now commit transaction"));

    // First restore autoCommit in case we hit an exception below:
    autoCommit = localAutoCommit;

    bool success = false;
    try {
        checkpoint();
        success = true;
    }
    _CLFINALLY(
            if (!success) {
                if (infoStream != NULL)
                    message(string("hit exception committing transaction"));

                rollbackTransaction();
            })

    if (!autoCommit)
        // Remove the incRef we did in startTransaction.
        deleter->decRef(localRollbackSegmentInfos);

    _CLDELETE(localRollbackSegmentInfos);

    // Give deleter a chance to remove files now:
    deleter->checkpoint(segmentInfos, autoCommit);
}

void IndexWriter::abort() {
    ensureOpen();
    if (autoCommit)
        _CLTHROWA(CL_ERR_IllegalState, "abort() can only be called when IndexWriter was opened with autoCommit=false");

    bool doClose;
    {
        SCOPED_LOCK_MUTEX(this->THIS_LOCK)
        // Ensure that only one thread actually gets to do the closing:
        if (!closing) {
            doClose = true;
            closing = true;
        } else
            doClose = false;
    }

    if (doClose) {

        finishMerges(false);

        // Must pre-close these two, in case they set
        // commitPending=true, so that we can then set it to
        // false before calling closeInternal
        mergePolicy->close();
        mergeScheduler->close();

        {
            SCOPED_LOCK_MUTEX(this->THIS_LOCK)
            // Keep the same segmentInfos instance but replace all
            // of its SegmentInfo instances.  This is so the next
            // attempt to commit using this instance of IndexWriter
            // will always write to a _CLNEW generation ("write
            // once").
            segmentInfos->clear();
            segmentInfos->insert(rollbackSegmentInfos, false);

            docWriter->abort(NULL);

            // Ask deleter to locate unreferenced files & remove
            // them:
            deleter->checkpoint(segmentInfos, false);
            deleter->refresh();
        }

        commitPending = false;
        closeInternal(false);
    } else
        waitForClose();
}

void IndexWriter::finishMerges(bool waitForMerges) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    if (!waitForMerges) {

        stopMerges = true;

        // Abort all pending & running merges:
        for (PendingMergesType::iterator it = pendingMerges->begin();
             it != pendingMerges->end(); it++) {
            MergePolicy::OneMerge *_merge = *it;
            if (infoStream != NULL)
                message("now abort pending merge " + _merge->segString(directory));
            _merge->abort();
            mergeFinish(_merge);

            it++;
        }
        pendingMerges->clear();

        for (RunningMergesType::iterator it = runningMerges->begin();
             it != runningMerges->end(); it++) {
            MergePolicy::OneMerge *_merge = *it;
            if (infoStream != NULL)
                message("now abort running merge " + _merge->segString(directory));
            _merge->abort();

            it++;
        }

        // These merges periodically check whether they have
        // been aborted, and stop if so.  We wait here to make
        // sure they all stop.  It should not take very int64_t
        // because the merge threads periodically check if
        // they are aborted.
        while (runningMerges->size() > 0) {
            if (infoStream != NULL)
                message(string("now wait for ") + Misc::toString((int32_t) runningMerges->size()) + " running merge to abort");
            CONDITION_WAIT(THIS_LOCK, THIS_WAIT_CONDITION)
        }

        assert(0 == mergingSegments->size());

        if (infoStream != NULL)
            message(string("all running merges have aborted"));

    } else {
        while (pendingMerges->size() > 0 || runningMerges->size() > 0) {
            CONDITION_WAIT(THIS_LOCK, THIS_WAIT_CONDITION)
        }
        assert(0 == mergingSegments->size());
    }
}

void IndexWriter::checkpoint() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    if (autoCommit) {
        segmentInfos->write(directory);
        commitPending = false;
        if (infoStream != NULL)
            message("checkpoint: wrote segments file \"" + segmentInfos->getCurrentSegmentFileName() + "\"");
    } else {
        commitPending = true;
    }
}

void IndexWriter::addIndexes(CL_NS(util)::ArrayBase<CL_NS(store)::Directory *> &dirs) {

    ensureOpen();

    // Do not allow add docs or deletes while we are running:
    docWriter->pauseAllThreads();

    try {

        if (infoStream != NULL)
            message(string("flush at addIndexes"));
        flush();

        bool success = false;

        startTransaction();

        try {

            {
                SCOPED_LOCK_MUTEX(this->THIS_LOCK)
                for (int32_t i = 0; i < dirs.length; i++) {
                    SegmentInfos sis;// read infos from dir
                    sis.read(dirs[i]);
                    segmentInfos->insert(&sis, true);// add each info
                }
            }

            optimize();

            success = true;
        }
        _CLFINALLY(
                if (success) {
                    commitTransaction();
                } else {
                    rollbackTransaction();
                })
    } catch (std::bad_alloc &) {
        hitOOM = true;
        _CLTHROWA(CL_ERR_OutOfMemory, "Out of memory");
    }
    _CLFINALLY(
            docWriter->resumeAllThreads();)
}

void IndexWriter::resetMergeExceptions() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    mergeExceptions->clear();
    mergeGen++;
}

void IndexWriter::indexCompaction(std::vector<lucene::store::Directory *> &src_dirs,
                                  std::vector<lucene::store::Directory *> dest_dirs,
                                  std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec,
                                  std::vector<uint32_t> dest_index_docs, bool maybe_skip) {
    CND_CONDITION(src_dirs.size() > 0, "Source directory not found.");
    CND_CONDITION(dest_dirs.size() > 0, "Destination directory not found.");
    this->_trans_vec = std::move(trans_vec);

    // create segment readers
    int numIndices = src_dirs.size();

    //Set of IndexReaders
    if (infoStream != nullptr) {
        message(string("src index dir size: ") + Misc::toString(numIndices));
    }

    // first level vector index is src_index_id
    // second level vector index is src_doc_id
    std::vector<std::vector<uint32_t>> srcNullBitmapValues(numIndices);
    IndexInput* null_bitmap_in = nullptr;
    for (int32_t i = 0; i < numIndices; i++) {
        // One index dir may have more than one segment, so we change the code to open all segments by using IndexReader::open
        // To keep the number of readers consistent with the number of src dirs.
        // Using IndexWriter::segmentInfos will be incorrect when there are more than one segment in one index dir
        IndexReader* reader = lucene::index::IndexReader::open(src_dirs[i], MERGE_READ_BUFFER_SIZE, false);
        readers.push_back(reader);
        if (infoStream != nullptr) {
            message(src_dirs[i]->toString());
        }

        // read null_bitmap and store values in srcBitmapValues
        try {
            if (src_dirs[i]->fileExists(NULL_BITMAP_FILE_NAME)) {
                // get null_bitmap index input
                null_bitmap_in = src_dirs[i]->openInput(NULL_BITMAP_FILE_NAME);
                size_t null_bitmap_size = null_bitmap_in->length();
                std::string buf;
                buf.resize(null_bitmap_size);
                null_bitmap_in->readBytes(reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data())), null_bitmap_size);
                auto null_bitmap = roaring::Roaring::read(buf.data(), false);
                null_bitmap.runOptimize();
                for (unsigned int v : null_bitmap) {
                    srcNullBitmapValues[i].emplace_back(v);
                }
                FINALLY_CLOSE_OUTPUT(null_bitmap_in);
            }
        } catch (CLuceneError &e) {
            FINALLY_CLOSE_OUTPUT(null_bitmap_in);
        }
    }
    assert(readers.size() == numIndices);

    // check hasProx
    bool hasProx = false;
    {
        if (!readers.empty()) {
            IndexReader* reader = readers[0];
            hasProx = reader->getFieldInfos()->hasProx();
            for (int32_t i = 1; i < readers.size(); i++) {
                if (hasProx != readers[i]->getFieldInfos()->hasProx()) {
                    _CLTHROWA(CL_ERR_IllegalArgument, "src_dirs hasProx inconformity");
                }
            }
        }
    }
    // std::cout << "hasProx: " << hasProx << std::endl;

    numDestIndexes = dest_dirs.size();

    // print dest index files
    if (infoStream != NULL) {
        message(string("dest index size: ") + Misc::toString(numDestIndexes));
        for (auto dest: dest_dirs) {
            message(dest->toString());
        }
    }

    // init new segment infos
    std::vector<SegmentInfo *> newSegmentInfos;
    SegmentInfo *newSegment = nullptr;
    std::string docStoreSegment;
    docStoreSegment.clear();

    std::vector<lucene::index::IndexWriter *> destIndexWriterList;
    std::vector<lucene::store::IndexOutput *> nullBitmapIndexOutputList;
    try {
        /// merge fields
        mergeFields(hasProx);

        /// write fields and create files writers
        for (int j = 0; j < numDestIndexes; j++) {
            auto dest_dir = dest_dirs[j];
            /// create dest index writers
            auto index_writer = _CLNEW IndexWriter(dest_dir, analyzer, true, true);
            destIndexWriterList.push_back(index_writer);

            std::string segment = index_writer->newSegmentName();
            // create segment info
            newSegment = _CLNEW SegmentInfo(segment.c_str(),
                                            (int) dest_index_docs[j],
                                            dest_dir, false, true,
                                            0, docStoreSegment.c_str(),
                                            false);
            newSegmentInfos.push_back(newSegment);

            /// write fields
            writeFields(dest_dir, segment);

            /// create file writers
            // Open an IndexOutput to the new Frequency File
            IndexOutput *freqOut = dest_dir->createOutput((Misc::segmentname(segment.c_str(), ".frq").c_str()));
            freqPointers.push_back(0);
            freqOutputList.push_back(freqOut);
            // Open an IndexOutput to the new Prox File
            IndexOutput *proxOut = nullptr;
            if (hasProx) {
                proxOut = dest_dir->createOutput(Misc::segmentname(segment.c_str(), ".prx").c_str());
                proxPointers.push_back(0);
            }
            proxOutputList.push_back(proxOut);
            // Instantiate a new termInfosWriter which will write in directory
            // for the segment name segment using the new merged fieldInfos
            TermInfosWriter *termInfosWriter = _CLNEW TermInfosWriter(dest_dir, segment.c_str(), fieldInfos, termIndexInterval);
            termInfosWriterList.push_back(termInfosWriter);
            // skipList writer
            skipInterval = termInfosWriter->skipInterval;
            maxSkipLevels = termInfosWriter->maxSkipLevels;
            skipListWriterList.push_back(_CLNEW DefaultSkipListWriter(skipInterval, maxSkipLevels, (int) dest_index_docs[j], freqOutputList[j], proxOutputList[j]));

            // create null_bitmap index output
            auto* null_bitmap_out = dest_dir->createOutput(NULL_BITMAP_FILE_NAME);
            nullBitmapIndexOutputList.push_back(null_bitmap_out);
        }

        /// merge terms
        mergeTerms(hasProx, maybe_skip);

        /// merge null_bitmap
        mergeNullBitmap(srcNullBitmapValues, nullBitmapIndexOutputList);
    } catch (CLuceneError &e) {
        throw e;
    }
    _CLFINALLY(
            for (auto freqOutput
                 : freqOutputList) {
                if (freqOutput != NULL) {
                    freqOutput->close();
                    _CLDELETE(freqOutput);
                }
            } freqOutputList.clear();
            for (auto proxOutput
                 : proxOutputList) {
                if (proxOutput != NULL) {
                    proxOutput->close();
                    _CLDELETE(proxOutput);
                }
            } proxOutputList.clear();
            for (auto termInfosWriter
                 : termInfosWriterList) {
                if (termInfosWriter != NULL) {
                    termInfosWriter->close();
                    _CLDELETE(termInfosWriter);
                }
            } termInfosWriterList.clear();
            for (auto skipListWriter
                 : skipListWriterList) {
                if (skipListWriter != NULL) {
                    _CLDELETE(skipListWriter);
                }
            } skipListWriterList.clear();
            for (auto r
                 : readers) {
                if (r != NULL) {
                    r->close();
                    _CLDELETE(r);
                }
            } readers.clear(););
            for (auto* null_bitmap_out
                 : nullBitmapIndexOutputList) {
                if (null_bitmap_out != nullptr) {
                    null_bitmap_out->close();
                    _CLDELETE(null_bitmap_out);
                }
            } nullBitmapIndexOutputList.clear();

    // update segment infos of dest index_writer in memory
    // close dest index writer
    for (int i = 0; i < numDestIndexes; i++) {
        auto index_writer = destIndexWriterList[i];
        index_writer->getSegmentInfos()->setElementAt(newSegmentInfos[i], 0);
        // close
        index_writer->close();
        _CLDELETE(index_writer);
    }
    destIndexWriterList.clear();

    // delete segment infos
    newSegmentInfos.clear();
}

void IndexWriter::compareIndexes(lucene::store::Directory *other) {
    /// compare merged segments
    // index compaction segments
    // term -> <docId, freq>
    std::map<lucene::index::Term *, std::pair<int, int>> merged_map;
    if (segmentInfos->size() == 0) {
        // reload segment infos from directory
        segmentInfos->read(directory);
    }

    std::vector<lucene::index::IndexReader *> index_readers;
    int32_t totDocCount = 0;
    for (int32_t i = 0; i < segmentInfos->size(); i++) {
        SegmentInfo *si = segmentInfos->info(i);
        IndexReader *reader = SegmentReader::get(si, MERGE_READ_BUFFER_SIZE, false /* mergeDocStores */);
        index_readers.push_back(reader);
        totDocCount += reader->numDocs();
    }

    lucene::index::IndexReader *merged_reader = index_readers[0];
    lucene::index::TermEnum *m_term_enum = merged_reader->terms();
    while (m_term_enum->next()) {
        lucene::index::Term *t = m_term_enum->term();
        lucene::index::TermPositions *postings = merged_reader->termPositions();
        postings->seek(t);
        std::pair<int, int> p;
        while (postings->next()) {
            int doc = postings->doc();
            int freq = postings->freq();
            p.first = doc;
            p.second = freq;
        }
        merged_map.emplace(t, p);
    }

    // new write segments
    std::map<lucene::index::Term *, std::pair<int, int>> new_map;
    lucene::index::SegmentInfos *new_sis;
    new_sis->read(other);

    std::vector<lucene::index::IndexReader *> new_index_readers;
    int32_t newTotCount = 0;
    for (int32_t i = 0; i < new_sis->size(); i++) {
        SegmentInfo *si = new_sis->info(i);
        IndexReader *reader = SegmentReader::get(si, MERGE_READ_BUFFER_SIZE, false /* mergeDocStores */);
        new_index_readers.push_back(reader);
        newTotCount += reader->numDocs();
    }

    if (totDocCount != newTotCount) {
        _CLTHROWA(CL_ERR_CorruptIndex, (string("docs count is not equal totCount(") + Misc::toString(totDocCount) +
                                        "), newTotCount(" + Misc::toString(newTotCount) + " )")
                                               .c_str());
    }

    lucene::index::IndexReader *new_reader = new_index_readers[0];
    lucene::index::TermEnum *n_term_enum = new_reader->terms();
    while (n_term_enum->next()) {
        lucene::index::Term *t = n_term_enum->term();
        lucene::index::TermPositions *postings = new_reader->termPositions();
        postings->seek(t);
        std::pair<int, int> p;
        while (postings->next()) {
            int doc = postings->doc();
            int freq = postings->freq();
            p.first = doc;
            p.second = freq;
        }
        new_map.emplace(t, p);
    }

    // compare
    for (auto m: merged_map) {
        lucene::index::Term *t = m.first;
        int doc = m.second.first;
        int freq = m.second.second;
        auto it = new_map.find(t);
        if (it != new_map.end()) {
            auto new_t = it->first;
            auto new_doc = it->second.first;
            auto new_freq = it->second.second;
            if (t->compareTo(new_t) != 0) {
                _CLTHROWA(CL_ERR_CorruptIndex, (string("term is not same, term(") + Misc::toString(t->toString()) +
                                                "), new_term(" + Misc::toString(new_t->toString()) + " )")
                                                       .c_str());
            }
            if (doc != new_doc) {
                _CLTHROWA(CL_ERR_CorruptIndex, (string("doc is not equal, term(") + Misc::toString(t->toString()) +
                                                "), doc(" + Misc::toString(doc) + "); new_term(" + Misc::toString(new_t->toString()) + " ), new_doc(" + Misc::toString(new_doc) + ")")
                                                       .c_str());
            }
            if (freq != new_freq) {
                _CLTHROWA(CL_ERR_CorruptIndex, (string("freq is not equal, term(") + Misc::toString(t->toString()) +
                                                "), freq(" + Misc::toString(freq) + "); new_term(" + Misc::toString(new_t->toString()) + " ), new_freq(" + Misc::toString(new_freq) + ")")
                                                       .c_str());
            }
        } else {
            _CLTHROWA(CL_ERR_CorruptIndex, (string("not found term(") + Misc::toString(t->toString())).c_str());
        }
    }
}

void IndexWriter::mergeFields(bool hasProx) {
    //Create a new FieldInfos
    fieldInfos = _CLNEW FieldInfos();
    //Condition check to see if fieldInfos points to a valid instance
    CND_CONDITION(fieldInfos != NULL, "Memory allocation for fieldInfos failed");

    //Condition check to see if reader points to a valid instanceL
    CND_CONDITION(readers.size() == 0, "No IndexReader found");
    // fields of all readers are the same, so we pick the first one.
    IndexReader *reader = readers[0];

    for (size_t j = 0; j < reader->getFieldInfos()->size(); j++) {
        FieldInfo *fi = reader->getFieldInfos()->fieldInfo(j);
        fieldInfos->add(fi->name, fi->isIndexed, fi->storeTermVector,
                        fi->storePositionWithTermVector, fi->storeOffsetWithTermVector,
                        !reader->hasNorms(fi->name), hasProx, fi->storePayloads);
    }
}

void IndexWriter::writeFields(lucene::store::Directory *d, std::string segment) {
    //Write the new FieldInfos file to the directory
    fieldInfos->write(d, Misc::segmentname(segment.c_str(), ".fnm").c_str());
}

struct DestDoc {
    uint32_t srcIdx{};
    uint32_t destIdx{};
    uint32_t destDocId{};
    uint32_t destFreq{};
    std::vector<uint32_t> destPositions{};

    DestDoc() = default;;
    DestDoc(uint32_t srcIdx, uint32_t destIdx, uint32_t destDocId) : srcIdx(srcIdx), destIdx(destIdx), destDocId(destDocId) {}
};

class postingQueue : public CL_NS(util)::PriorityQueue<DestDoc*,CL_NS(util)::Deletor::Object<DestDoc> >{
public:
    explicit postingQueue(int32_t count)
    {
        initialize(count, false);
    }
    ~postingQueue() override{
        close();
    }

    void close() {
        clear();
    }
protected:
    bool lessThan(DestDoc* a, DestDoc* b) override {
        if (a->destIdx == b->destIdx) {
            return a->destDocId < b->destDocId;
        } else {
            return a->destIdx < b->destIdx;
        }
    }

};

void IndexWriter::mergeTerms(bool hasProx, bool maybe_skip) {
    auto queue = _CLNEW SegmentMergeQueue(readers.size());
    auto numSrcIndexes = readers.size();
    //std::vector<TermPositions *> postingsList(numSrcIndexes);


    int32_t base = 0;
    IndexReader *reader = nullptr;
    SegmentMergeInfo *smi = nullptr;

    for (int i = 0; i < numSrcIndexes; ++i) {
        reader = readers[i];

        TermEnum *termEnum = reader->terms();
        smi = _CLNEW SegmentMergeInfo(base, termEnum, reader, i);

        base += reader->numDocs();
        if (smi->next()) {
            queue->put(smi);
        } else {
            smi->close();
            _CLDELETE(smi);
        }
    }
    auto **match = _CL_NEWARRAY(SegmentMergeInfo *, readers.size());

    while (queue->size() > 0) {
        int32_t matchSize = 0;

        match[matchSize++] = queue->pop();
        Term *smallestTerm = match[0]->term;
        SegmentMergeInfo *top = queue->top();
        if (infoStream != nullptr) {
            std::string name = lucene_wcstoutf8string(smallestTerm->text(), smallestTerm->textLength());
            std::string field = lucene_wcstoutf8string(smallestTerm->field(), wcslen(smallestTerm->field()));
            message("smallestTerm name: " + name);
            message("smallestTerm field: " + field);

            if (top != nullptr) {
                Term* topTerm = top->term;
                std::string name1 = lucene_wcstoutf8string(topTerm->text(), topTerm->textLength());
                std::string field1 = lucene_wcstoutf8string(topTerm->field(), wcslen(topTerm->field()));
                message("topTerm name: " + name1);
                message("topTerm field: " + field1);
            }
        }
        while (top != nullptr && smallestTerm->equals(top->term)) {
            match[matchSize++] = queue->pop();
            top = queue->top();
        }

        if (maybe_skip && smallestTerm) {
            auto containsUpperCase = [](const std::wstring_view& ws_term) {
                return std::any_of(ws_term.begin(), ws_term.end(),
                                   [](wchar_t ch) { return std::iswupper(ch) != 0; });
            };

            std::wstring_view ws_term(smallestTerm->text(), smallestTerm->textLength());
            if (containsUpperCase(ws_term)) {
                _CLTHROWA(CL_ERR_InvalidState, "need rewrite, skip index compaction");
            }
        }

        std::vector<std::vector<uint32_t>> docDeltaBuffers(numDestIndexes);
        std::vector<std::vector<uint32_t>> freqBuffers(numDestIndexes);
        auto destPostingQueues = _CLNEW postingQueue(matchSize);
        std::vector<DestDoc> destDocs(matchSize);

        auto processPostings = [&](TermPositions* postings, DestDoc* destDoc, int srcIdx) {
            while (postings->next()) {
                int srcDoc = postings->doc();
                std::pair<int32_t, uint32_t> p = _trans_vec[smi->readerIndex][srcDoc];
                destDoc->destIdx = p.first;
                destDoc->destDocId = p.second;
                destDoc->srcIdx = srcIdx;
                // <UINT32_MAX, UINT32_MAX> indicates current row not exist in Doris dest segment.
                // So we ignore this doc here.
                if (destDoc->destIdx == UINT32_MAX || destDoc->destDocId == UINT32_MAX) {
                    if (infoStream != nullptr) {
                        std::stringstream ss;
                        ss << "skip UINT32_MAX, srcIdx: " << smi->readerIndex << ", srcDoc: " << srcDoc
                           << ", destIdx: " << destDoc->destIdx << ", destDocId: " << destDoc->destDocId;
                        message(ss.str());
                    }
                    continue;
                }

                if (hasProx) {
                    int32_t freq = postings->freq();
                    destDoc->destFreq = freq;
                    destDoc->destPositions.resize(freq);

                    for (int32_t j = 0; j < freq; j++) {
                        int32_t position = postings->nextPosition();
                        destDoc->destPositions[j] = position;
                    }
                }

                destPostingQueues->put(destDoc);
                break;
            }
        };

        for (int i = 0; i < matchSize; ++i) {
            smi = match[i];
            auto* postings = smi->getPositions();
            postings->seek(smi->termEnum);
            processPostings(postings, &destDocs[i], i);
        }

        auto encode = [](IndexOutput* out, std::vector<uint32_t>& buffer, bool isDoc) {
            std::vector<uint8_t> compress(4 * buffer.size() + PFOR_BLOCK_SIZE);
            size_t size = 0;
            if (isDoc) {
                size = P4ENC(buffer.data(), buffer.size(), compress.data());
            } else {
                size = P4NZENC(buffer.data(), buffer.size(), compress.data());
            }
            out->writeVInt(size);
            out->writeBytes(reinterpret_cast<const uint8_t*>(compress.data()), size);
            buffer.resize(0);
        };

        std::vector<int32_t> dfs(numDestIndexes, 0);
        std::vector<int32_t> lastDocs(numDestIndexes, 0);
        while (destPostingQueues->size()) {
            if (destPostingQueues->top() != nullptr) {
                auto destDoc = destPostingQueues->pop();
                auto destIdx = destDoc->destIdx;
                auto destDocId = destDoc->destDocId;
                auto destFreq = destDoc->destFreq;
                auto& descPositions = destDoc->destPositions;
                if (infoStream != nullptr) {
                    for (int i = 0; i < _trans_vec.size(); ++i) {
                        // find pair < destIdx, destDocId > in _trans_vec[i] to get the index of the pair
                        auto it = std::find_if(_trans_vec[i].begin(), _trans_vec[i].end(),
                                               [destIdx, destDocId](const std::pair<uint32_t, uint32_t>& pair) {
                                                   return pair.first == destIdx && pair.second == destDocId;
                                               });

                        // Check if the pair was found
                        if (it != _trans_vec[i].end()) {
                            // Calculate the index of the pair
                            size_t index = std::distance(_trans_vec[i].begin(), it);
                            std::stringstream ss;
                            ss << "Found pair at srcIdxId:" << i << ", srcDocId: " << index << ", destIdxId: " << destIdx
                               << ", destDocId: " << destDocId << ", destFreq: " << destDoc->destFreq;
                            message(ss.str());
                        }
                    }
                }

                auto freqOut = freqOutputList[destIdx];
                auto proxOut = proxOutputList[destIdx];
                auto& docDeltaBuffer = docDeltaBuffers[destIdx];
                auto& freqBuffer = freqBuffers[destIdx];
                auto skipWriter = skipListWriterList[destIdx];
                auto& df = dfs[destIdx];
                auto& lastDoc = lastDocs[destIdx];

                if (df == 0) {
                    freqPointers[destIdx] = freqOut->getFilePointer();
                    if (hasProx) {
                        proxPointers[destIdx] = proxOut->getFilePointer();
                    }
                    skipWriter->resetSkip();
                }

                if ((++df % skipInterval) == 0) {
                    freqOut->writeByte((char)CodeMode::kPfor);
                    freqOut->writeVInt(docDeltaBuffer.size());
                    encode(freqOut, docDeltaBuffer, true);
                    if (hasProx) {
                        encode(freqOut, freqBuffer, false);
                    }

                    skipWriter->setSkipData(lastDoc, false, -1);
                    skipWriter->bufferSkip(df);
                }

                assert(destDocId > lastDoc || df == 1);
                lastDoc = destDocId;

                docDeltaBuffer.push_back(destDocId);
                if (hasProx) {
                    int32_t lastPosition = 0;
                    for (int32_t i = 0; i < descPositions.size(); i++) {
                        int32_t position = descPositions[i];
                        int32_t delta = position - lastPosition;
                        proxOut->writeVInt(delta);
                        lastPosition = position;
                    }
                    freqBuffer.push_back(destFreq);
                }

                smi = match[destDoc->srcIdx];
                processPostings(smi->getPositions(), destDoc, destDoc->srcIdx);
            }
        }
        if (destPostingQueues != nullptr) {
            destPostingQueues->close();
            _CLDELETE(destPostingQueues);
        }
        
        if (infoStream != nullptr) {
            std::stringstream ss;
            for (const auto& df : dfs) {
                ss<< "df: " << df << "\n";
            }
            message(ss.str());
        }

        for (int i = 0; i < numDestIndexes; ++i) {
            DefaultSkipListWriter *skipListWriter = skipListWriterList[i];
            CL_NS(store)::IndexOutput *freqOutput = freqOutputList[i];
            CL_NS(store)::IndexOutput *proxOutput = proxOutputList[i];
            TermInfosWriter *termInfosWriter = termInfosWriterList[i];
            int64_t freqPointer = freqPointers[i];
            int64_t proxPointer = 0;
            if (hasProx) {
                proxPointer = proxPointers[i];
            }

            {
                auto& docDeltaBuffer = docDeltaBuffers[i];
                auto& freqBuffer = freqBuffers[i];

                freqOutput->writeByte((char)CodeMode::kDefault);
                freqOutput->writeVInt(docDeltaBuffer.size());
                uint32_t lastDoc = 0;
                for (int32_t i = 0; i < docDeltaBuffer.size(); i++) {
                    uint32_t curDoc = docDeltaBuffer[i];
                    if (hasProx) {
                        uint32_t newDocCode = (docDeltaBuffer[i] - lastDoc) << 1;
                        lastDoc = curDoc;
                        uint32_t freq = freqBuffer[i];
                        if (1 == freq) {
                            freqOutput->writeVInt(newDocCode | 1);
                        } else {
                            freqOutput->writeVInt(newDocCode);
                            freqOutput->writeVInt(freq);
                        }
                    } else {
                        freqOutput->writeVInt(curDoc - lastDoc);
                        lastDoc = curDoc;
                    }
                }
                docDeltaBuffer.resize(0);
                freqBuffer.resize(0);
            }
            
            int64_t skipPointer = skipListWriter->writeSkip(freqOutput);

            // write terms
            TermInfo termInfo;
            termInfo.set(dfs[i], freqPointer, proxPointer, (int32_t) (skipPointer - freqPointer));
            // Write a new TermInfo
            termInfosWriter->add(smallestTerm, &termInfo);
        }

        while (matchSize > 0) {
            smi = match[--matchSize];

            // Move to the next term in the enumeration of SegmentMergeInfo smi
            if (smi->next()) {
                // There still are some terms so restore smi in the queue
                queue->put(smi);

            } else {
                // Done with a segment
                // No terms anymore so close this SegmentMergeInfo instance
                smi->close();
                _CLDELETE(smi);
            }
        }
    }

    _CLDELETE_ARRAY(match);
    if (queue != NULL) {
        queue->close();
        _CLDELETE(queue);
    }
}

void IndexWriter::mergeNullBitmap(std::vector<std::vector<uint32_t>> srcNullBitmapValues, std::vector<lucene::store::IndexOutput *> nullBitmapIndexOutputList) {
    // first level vector index is dest_index_id
    // second level vector index is dest_doc_id
    std::vector<std::vector<uint32_t>> destNullBitmapValues(numDestIndexes);

    // iterate srcNullBitmapValues to construct destNullBitmapValues
    for (size_t i = 0; i < srcNullBitmapValues.size(); ++i) {
        std::vector<uint32_t> &indexSrcBitmapValues = srcNullBitmapValues[i];
        if (indexSrcBitmapValues.empty()) {
            // empty indicates there is no null_bitmap file in this index
            continue;
        }
        for (const auto& srcDocId : indexSrcBitmapValues) {
            auto destIdx = _trans_vec[i][srcDocId].first;
            auto destDocId = _trans_vec[i][srcDocId].second;
            // <UINT32_MAX, UINT32_MAX> indicates current row not exist in Doris dest segment.
            // So we ignore this doc here.
            if (destIdx == UINT32_MAX || destDocId == UINT32_MAX) {
                continue;
            }
            destNullBitmapValues[destIdx].emplace_back(destDocId);
        }
    }

    // construct null_bitmap and write null_bitmap to dest index
    for (size_t i = 0; i < destNullBitmapValues.size(); ++i) {
        roaring::Roaring null_bitmap;
        for (const auto& v : destNullBitmapValues[i]) {
            null_bitmap.add(v);
        }
        // write null_bitmap file
        auto* nullBitmapIndexOutput = nullBitmapIndexOutputList[i];
        null_bitmap.runOptimize();
        size_t size = null_bitmap.getSizeInBytes(false);
        if (size > 0) {
            std::string buf;
            buf.resize(size);
            null_bitmap.write(reinterpret_cast<char*>(buf.data()), false);
            nullBitmapIndexOutput->writeBytes(reinterpret_cast<uint8_t*>(buf.data()), size);
        }
    }
}

void IndexWriter::addIndexesNoOptimize(CL_NS(util)::ArrayBase<CL_NS(store)::Directory *> &dirs) {
    ensureOpen();

    // Do not allow add docs or deletes while we are running:
    docWriter->pauseAllThreads();

    try {
        if (infoStream != NULL)
            message(string("flush at addIndexesNoOptimize"));
        flush();

        bool success = false;

        startTransaction();

        try {

            {
                SCOPED_LOCK_MUTEX(this->THIS_LOCK)
                for (int32_t i = 0; i < dirs.length; i++) {
                    if (directory == dirs[i]) {
                        // cannot add this index: segments may be deleted in merge before added
                        _CLTHROWA(CL_ERR_IllegalArgument, "Cannot add this index to itself");
                    }

                    SegmentInfos sis;// read infos from dir
                    sis.read(dirs[i]);
                    segmentInfos->insert(&sis, true);
                }
            }

            maybeMerge();

            // If after merging there remain segments in the index
            // that are in a different directory, just copy these
            // over into our index.  This is necessary (before
            // finishing the transaction) to avoid leaving the
            // index in an unusable (inconsistent) state.
            copyExternalSegments();

            success = true;
        }
        _CLFINALLY(
                if (success) {
                    commitTransaction();
                } else {
                    rollbackTransaction();
                })
    } catch (std::bad_alloc &) {
        hitOOM = true;
        _CLTHROWA(CL_ERR_OutOfMemory, "Out of memory");
    }
    _CLFINALLY(
            docWriter->resumeAllThreads();)
}

void IndexWriter::copyExternalSegments() {

    bool any = false;

    while (true) {
        SegmentInfo *info = NULL;
        MergePolicy::OneMerge *_merge = NULL;
        {
            SCOPED_LOCK_MUTEX(this->THIS_LOCK)
            const int32_t numSegments = segmentInfos->size();
            for (int32_t i = 0; i < numSegments; i++) {
                info = segmentInfos->info(i);
                if (info->dir != directory) {
                    SegmentInfos *range = _CLNEW SegmentInfos;
                    segmentInfos->range(i, 1 + i, *range);
                    _merge = _CLNEW MergePolicy::OneMerge(range, info->getUseCompoundFile());
                    break;
                }
            }
        }

        if (_merge != NULL) {
            if (registerMerge(_merge)) {
                PendingMergesType::iterator p = std::find(pendingMerges->begin(), pendingMerges->end(), _merge);
                pendingMerges->remove(p, true);
                runningMerges->insert(_merge);
                any = true;
                merge(_merge);
            } else
                // This means there is a bug in the
                // MergeScheduler.  MergeSchedulers in general are
                // not allowed to run a merge involving segments
                // external to this IndexWriter's directory in the
                // background because this would put the index
                // into an inconsistent state (where segmentInfos
                // has been written with such external segments
                // that an IndexReader would fail to load).
                _CLTHROWA(CL_ERR_Merge, (string("segment \"") + info->name + " exists in external directory yet the MergeScheduler executed the merge in a separate thread").c_str());
        } else
            // No more external segments
            break;
    }

    if (any)
        // Sometimes, on copying an external segment over,
        // more merges may become necessary:
        mergeScheduler->merge(this);
}

void IndexWriter::doAfterFlush() {
}

void IndexWriter::flush() {
    flush(true, false);
}

void IndexWriter::flush(bool triggerMerge, bool _flushDocStores) {
    ensureOpen();

    if (doFlush(_flushDocStores) && triggerMerge)
        maybeMerge();
}

bool IndexWriter::doFlush(bool _flushDocStores) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)

    // Make sure no threads are actively adding a document

    // Returns true if docWriter is currently aborting, in
    // which case we skip flushing this segment
    /*if (docWriter->pauseAllThreads()) {
    docWriter->resumeAllThreads();
    return false;
  }*/

    bool ret = false;
    try {

        SegmentInfo *newSegment = NULL;

        const int32_t numDocs = docWriter->getNumDocsInRAM();
        //const int32_t numDocs = sdocWriter->getNumDocsInRAM();


        // Always flush docs if there are any
        bool flushDocs = numDocs > 0;

        // With autoCommit=true we always must flush the doc
        // stores when we flush
        _flushDocStores |= autoCommit;
        //string docStoreSegment = sdocWriter->getDocStoreSegment();
        string docStoreSegment = docWriter->getDocStoreSegment();
        if (docStoreSegment.empty())
            _flushDocStores = false;

        // Always flush deletes if there are any delete terms.
        // TODO: when autoCommit=false we don't have to flush
        // deletes with every flushed segment; we can save
        // CPU/IO by buffering longer & flushing deletes only
        // when they are full or writer is being closed.  We
        // have to fix the "applyDeletesSelectively" logic to
        // apply to more than just the last flushed segment
        //bool flushDeletes = sdocWriter->hasDeletes();
        bool flushDeletes = docWriter->hasDeletes();

        if (infoStream != NULL) {
            message("  flush: segment=" + docWriter->getSegment() +
                    " docStoreSegment=" + docWriter->getDocStoreSegment() +
                    " docStoreOffset=" + Misc::toString(docWriter->getDocStoreOffset()) +
                    " flushDocs=" + Misc::toString(flushDocs) +
                    " flushDeletes=" + Misc::toString(flushDeletes) +
                    " flushDocStores=" + Misc::toString(_flushDocStores) +
                    " numDocs=" + Misc::toString(numDocs) +
                    " numBufDelTerms=" + Misc::toString(docWriter->getNumBufferedDeleteTerms()));
            message("  index before flush " + segString());
        }

        //int32_t docStoreOffset = sdocWriter->getDocStoreOffset();
        int32_t docStoreOffset = docWriter->getDocStoreOffset();

        // docStoreOffset should only be non-zero when
        // autoCommit == false
        assert(!autoCommit || 0 == docStoreOffset);

        bool docStoreIsCompoundFile = false;

        // Check if the doc stores must be separately flushed
        // because other segments, besides the one we are about
        // to flush, reference it
        //if (_flushDocStores && (!flushDocs || !sdocWriter->getSegment().compare(sdocWriter->getDocStoreSegment())==0 )) {
        if (_flushDocStores && (!flushDocs || !docWriter->getSegment().compare(docWriter->getDocStoreSegment()) == 0)) {
            // We must separately flush the doc store
            if (infoStream != NULL)
                message("  flush shared docStore segment " + docStoreSegment);

            docStoreIsCompoundFile = flushDocStores();
            _flushDocStores = false;
        }

        //string segment = sdocWriter->getSegment();
        string segment = docWriter->getSegment();

        // If we are flushing docs, segment must not be NULL:
        assert(!segment.empty() || !flushDocs);

        if (flushDocs || flushDeletes) {

            SegmentInfos *rollback = NULL;

            if (flushDeletes)
                rollback = segmentInfos->clone();

            bool success = false;

            try {
                if (flushDocs) {

                    if (0 == docStoreOffset && _flushDocStores) {
                        // This means we are flushing doc stores
                        // with this segment, so it will not be shared
                        // with other segments
                        assert(!docStoreSegment.empty());
                        assert(docStoreSegment.compare(segment) == 0);
                        docStoreOffset = -1;
                        docStoreIsCompoundFile = false;
                        docStoreSegment.clear();
                    }
                    //int32_t flushedDocCount = sdocWriter->flush(_flushDocStores);
                    int32_t flushedDocCount = docWriter->flush(_flushDocStores);

                    newSegment = _CLNEW SegmentInfo(segment.c_str(),
                                                    flushedDocCount,
                                                    directory, false, true,
                                                    docStoreOffset, docStoreSegment.c_str(),
                                                    docStoreIsCompoundFile);
                    segmentInfos->insert(newSegment);
                }

                if (flushDeletes)
                    // we should be able to change this so we can
                    // buffer deletes longer and then flush them to
                    // multiple flushed segments, when
                    // autoCommit=false
                    applyDeletes(flushDocs);

                doAfterFlush();

                checkpoint();
                success = true;
            }
            _CLFINALLY(
                    if (!success) {
                        if (infoStream != NULL)
                            message("hit exception flushing segment " + segment);

                        if (flushDeletes) {

                            // Carefully check if any partial .del files
                            // should be removed:
                            const int32_t size = rollback->size();
                            for (int32_t i = 0; i < size; i++) {
                                const string newDelFileName = segmentInfos->info(i)->getDelFileName();
                                const string delFileName = rollback->info(i)->getDelFileName();
                                if (!newDelFileName.empty() && newDelFileName.compare(delFileName) != 0)
                                    deleter->deleteFile(newDelFileName.c_str());
                            }

                            // Fully replace the segmentInfos since flushed
                            // deletes could have changed any of the
                            // SegmentInfo instances:
                            segmentInfos->clear();
                            assert(false);//test me..
                            segmentInfos->insert(rollback, false);

                        } else {
                            // Remove segment we added, if any:
                            if (newSegment != NULL &&
                                segmentInfos->size() > 0 &&
                                segmentInfos->info(segmentInfos->size() - 1) == newSegment)
                                segmentInfos->remove(segmentInfos->size() - 1);
                        }
                        if (flushDocs) {}
                        //sdocWriter->abort(NULL);
                        deletePartialSegmentsFile();
                        deleter->checkpoint(segmentInfos, false);

                        if (!segment.empty())
                            deleter->refresh(segment.c_str());
                    } else if (flushDeletes)
                            _CLDELETE(rollback);)

            deleter->checkpoint(segmentInfos, autoCommit);

            if (flushDocs && mergePolicy->useCompoundFile(segmentInfos,
                                                          newSegment)) {
                success = false;
                try {
                    docWriter->createCompoundFile(segment);
                    newSegment->setUseCompoundFile(true);
                    checkpoint();
                    success = true;
                }
                _CLFINALLY(
                        if (!success) {
                            if (infoStream != NULL)
                                message("hit exception creating compound file for newly flushed segment " + segment);
                            newSegment->setUseCompoundFile(false);
                            deleter->deleteFile((segment + "." + IndexFileNames::COMPOUND_FILE_EXTENSION).c_str());
                            deletePartialSegmentsFile();
                        })

                deleter->checkpoint(segmentInfos, autoCommit);
            }

            ret = true;
        }

    } catch (std::bad_alloc &) {
        hitOOM = true;
        _CLTHROWA(CL_ERR_OutOfMemory, "Out of memory");
    }
    _CLFINALLY(
            //docWriter->clearFlushPending();
            //docWriter->resumeAllThreads();
    )
    return ret;
}

int64_t IndexWriter::ramSizeInBytes() {
    ensureOpen();
    return docWriter->getRAMUsed();
}

int32_t IndexWriter::numRamDocs() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    ensureOpen();
    return docWriter->getNumDocsInRAM();
}

int32_t IndexWriter::ensureContiguousMerge(MergePolicy::OneMerge *_merge) {

    int32_t first = segmentInfos->indexOf(_merge->segments->info(0));
    if (first == -1)
        _CLTHROWA(CL_ERR_Merge, (string("could not find segment ") + _merge->segments->info(0)->name + " in current segments").c_str());

    const int32_t numSegments = segmentInfos->size();

    const int32_t numSegmentsToMerge = _merge->segments->size();
    for (int32_t i = 0; i < numSegmentsToMerge; i++) {
        const SegmentInfo *info = _merge->segments->info(i);

        if (first + i >= numSegments || !segmentInfos->info(first + i)->equals(info)) {
            if (segmentInfos->indexOf(info) == -1)
                _CLTHROWA(CL_ERR_Merge, (string("MergePolicy selected a segment (") + info->name + ") that is not in the index").c_str());
            else
                _CLTHROWA(CL_ERR_Merge, (string("MergePolicy selected non-contiguous segments to merge (") + _merge->getObjectName() + " vs " + segString() + "), which IndexWriter (currently) cannot handle").c_str());
        }
    }

    return first;
}

bool IndexWriter::commitMerge(MergePolicy::OneMerge *_merge) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)

    assert(_merge->registerDone);

    if (hitOOM)
        return false;

    if (infoStream != NULL)
        message("commitMerge: " + _merge->segString(directory));

    // If merge was explicitly aborted, or, if abort() or
    // rollbackTransaction() had been called since our merge
    // started (which results in an unqualified
    // deleter->refresh() call that will remove any index
    // file that current segments does not reference), we
    // abort this merge
    if (_merge->isAborted()) {
        if (infoStream != NULL)
            message("commitMerge: skipping merge " + _merge->segString(directory) + ": it was aborted");

        assert(_merge->increfDone);
        decrefMergeSegments(_merge);
        deleter->refresh(_merge->info->name.c_str());
        return false;
    }

    bool success = false;

    int32_t start;

    try {
        SegmentInfos *sourceSegmentsClone = _merge->segmentsClone;
        const SegmentInfos *sourceSegments = _merge->segments;

        start = ensureContiguousMerge(_merge);
        if (infoStream != NULL)
            message("commitMerge " + _merge->segString(directory));

        // Carefully merge deletes that occurred after we
        // started merging:

        BitVector *deletes = NULL;
        int32_t docUpto = 0;

        const int32_t numSegmentsToMerge = sourceSegments->size();
        for (int32_t i = 0; i < numSegmentsToMerge; i++) {
            const SegmentInfo *previousInfo = sourceSegmentsClone->info(i);
            const SegmentInfo *currentInfo = sourceSegments->info(i);

            assert(currentInfo->docCount == previousInfo->docCount);

            const int32_t docCount = currentInfo->docCount;

            if (previousInfo->hasDeletions()) {

                // There were deletes on this segment when the merge
                // started.  The merge has collapsed away those
                // deletes, but, if _CLNEW deletes were flushed since
                // the merge started, we must now carefully keep any
                // newly flushed deletes but mapping them to the _CLNEW
                // docIDs.

                assert(currentInfo->hasDeletions());

                // Load deletes present @ start of merge, for this segment:
                BitVector previousDeletes(previousInfo->dir, previousInfo->getDelFileName().c_str());

                if (!currentInfo->getDelFileName().compare(previousInfo->getDelFileName()) == 0) {
                    // This means this segment has had new deletes
                    // committed since we started the merge, so we
                    // must merge them:
                    if (deletes == NULL)
                        deletes = _CLNEW BitVector(_merge->info->docCount);

                    BitVector currentDeletes(currentInfo->dir, currentInfo->getDelFileName().c_str());
                    for (int32_t j = 0; j < docCount; j++) {
                        if (previousDeletes.get(j))
                            assert(currentDeletes.get(j));
                        else {
                            if (currentDeletes.get(j))
                                deletes->set(docUpto);
                            docUpto++;
                        }
                    }
                } else
                    docUpto += docCount - previousDeletes.count();

            } else if (currentInfo->hasDeletions()) {
                // This segment had no deletes before but now it
                // does:
                if (deletes == NULL)
                    deletes = _CLNEW BitVector(_merge->info->docCount);
                BitVector currentDeletes(directory, currentInfo->getDelFileName().c_str());

                for (int32_t j = 0; j < docCount; j++) {
                    if (currentDeletes.get(j))
                        deletes->set(docUpto);
                    docUpto++;
                }

            } else
                // No deletes before or after
                docUpto += currentInfo->docCount;

            _merge->checkAborted(directory);
        }

        if (deletes != NULL) {
            _merge->info->advanceDelGen();
            deletes->write(directory, _merge->info->getDelFileName().c_str());
            _CLDELETE(deletes);
        }
        success = true;
    }
    _CLFINALLY(
            if (!success) {
                if (infoStream != NULL)
                    message(string("hit exception creating merged deletes file"));
                deleter->refresh(_merge->info->name.c_str());
            })

    // Simple optimization: if the doc store we are using
    // has been closed and is in now compound format (but
    // wasn't when we started), then we will switch to the
    // compound format as well:
    const string mergeDocStoreSegment = _merge->info->getDocStoreSegment();
    if (!mergeDocStoreSegment.empty() && !_merge->info->getDocStoreIsCompoundFile()) {
        const int32_t size = segmentInfos->size();
        for (int32_t i = 0; i < size; i++) {
            const SegmentInfo *info = segmentInfos->info(i);
            const string docStoreSegment = info->getDocStoreSegment();
            if (!docStoreSegment.empty() &&
                docStoreSegment.compare(mergeDocStoreSegment) == 0 &&
                info->getDocStoreIsCompoundFile()) {
                _merge->info->setDocStoreIsCompoundFile(true);
                break;
            }
        }
    }

    success = false;
    SegmentInfos *rollback = NULL;
    try {
        rollback = segmentInfos->clone();
        int32_t segmentssize = _merge->segments->size();
        for (int32_t i = 0; i < segmentssize; i++) {
            segmentInfos->remove(start);
        }
        segmentInfos->add(_merge->info, start);
        checkpoint();
        success = true;
    }
    _CLFINALLY(
            if (!success && rollback != NULL) {
                if (infoStream != NULL)
                    message(string("hit exception when checkpointing after merge"));
                segmentInfos->clear();
                segmentInfos->insert(rollback, true);
                deletePartialSegmentsFile();
                deleter->refresh(_merge->info->name.c_str());
            } _CLDELETE(rollback);)

    if (_merge->optimize)
        segmentsToOptimize->push_back(_merge->info);

    // Must checkpoint before decrefing so any newly
    // referenced files in the _CLNEW merge->info are incref'd
    // first:
    deleter->checkpoint(segmentInfos, autoCommit);

    decrefMergeSegments(_merge);

    return true;
}


void IndexWriter::decrefMergeSegments(MergePolicy::OneMerge *_merge) {
    const SegmentInfos *sourceSegmentsClone = _merge->segmentsClone;
    const int32_t numSegmentsToMerge = sourceSegmentsClone->size();
    assert(_merge->increfDone);
    _merge->increfDone = false;
    for (int32_t i = 0; i < numSegmentsToMerge; i++) {
        SegmentInfo *previousInfo = sourceSegmentsClone->info(i);
        // Decref all files for this SegmentInfo (this
        // matches the incref in mergeInit):
        if (previousInfo->dir == directory)
            deleter->decRef(previousInfo->files());
    }
}

void IndexWriter::merge(MergePolicy::OneMerge *_merge) {

    assert(_merge->registerDone);
    assert(!_merge->optimize || _merge->maxNumSegmentsOptimize > 0);

    bool success = false;

    try {
        try {
            try {
                mergeInit(_merge);

                if (infoStream != NULL)
                    message("now merge\n  merge=" + _merge->segString(directory) + "\n  index=" + segString());

                mergeMiddle(_merge);
                success = true;
            } catch (CLuceneError &e) {
                if (e.number() != CL_ERR_MergeAborted) throw e;
                _merge->setException(e);
                addMergeException(_merge);
                // We can ignore this exception, unless the merge
                // involves segments from external directories, in
                // which case we must throw it so, for example, the
                // rollbackTransaction code in addIndexes* is
                // executed.
                if (_merge->isExternal)
                    throw e;
            }
        }
        _CLFINALLY(
                {
                    SCOPED_LOCK_MUTEX(this->THIS_LOCK)
                    try {

                        mergeFinish(_merge);

                        if (!success) {
                            if (infoStream != NULL)
                                message(string("hit exception during merge"));
                            addMergeException(_merge);
                            if (_merge->info != NULL && segmentInfos->indexOf(_merge->info) == -1)
                                deleter->refresh(_merge->info->name.c_str());
                        }

                        // This merge (and, generally, any change to the
                        // segments) may now enable new merges, so we call
                        // merge policy & update pending merges.
                        if (success && !_merge->isAborted() && !closed && !closing)
                            updatePendingMerges(_merge->maxNumSegmentsOptimize, _merge->optimize);
                    }
                    _CLFINALLY(
                            RunningMergesType::iterator itr = runningMerges->find(_merge);
                            if (itr != runningMerges->end()) runningMerges->remove(itr);
                            // Optimize may be waiting on the final optimize
                            // merge to finish; and finishMerges() may be
                            // waiting for all merges to finish:
                            CONDITION_NOTIFYALL(THIS_WAIT_CONDITION))
                })
    } catch (std::bad_alloc &) {
        hitOOM = true;
        _CLTHROWA(CL_ERR_OutOfMemory, "Out of memory");
    }
}

bool IndexWriter::registerMerge(MergePolicy::OneMerge *_merge) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)

    if (_merge->registerDone)
        return true;

    const int32_t count = _merge->segments->size();
    bool isExternal = false;
    for (int32_t i = 0; i < count; i++) {
        SegmentInfo *info = _merge->segments->info(i);
        if (mergingSegments->find(info) != mergingSegments->end())
            return false;
        if (segmentInfos->indexOf(info) == -1)
            return false;
        if (info->dir != directory)
            isExternal = true;
    }

    pendingMerges->push_back(_merge);

    if (infoStream != NULL)
        message(string("add merge to pendingMerges: ") + _merge->segString(directory) + " [total " + Misc::toString((int32_t) pendingMerges->size()) + " pending]");

    _merge->mergeGen = mergeGen;
    _merge->isExternal = isExternal;

    // OK it does not conflict; now record that this merge
    // is running (while synchronized) to avoid race
    // condition where two conflicting merges from different
    // threads, start
    for (int32_t i = 0; i < count; i++)
        mergingSegments->insert(mergingSegments->end(), _merge->segments->info(i));

    // Merge is now registered
    _merge->registerDone = true;
    return true;
}

void IndexWriter::mergeInit(MergePolicy::OneMerge *_merge) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    bool success = false;
    try {
        _mergeInit(_merge);
        success = true;
    }
    _CLFINALLY(
            if (!success) {
                mergeFinish(_merge);
                RunningMergesType::iterator itr = runningMerges->find(_merge);
                if (itr != runningMerges->end()) runningMerges->remove(itr);
            })
}

void IndexWriter::_mergeInit(MergePolicy::OneMerge *_merge) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)

    assert(testPoint("startMergeInit"));

    assert(_merge->registerDone);

    if (_merge->info != NULL)
        // mergeInit already done
        return;

    if (_merge->isAborted())
        return;

    const SegmentInfos *sourceSegments = _merge->segments;
    const int32_t end = sourceSegments->size();

    ensureContiguousMerge(_merge);

    // Check whether this merge will allow us to skip
    // merging the doc stores (stored field & vectors).
    // This is a very substantial optimization (saves tons
    // of IO) that can only be applied with
    // autoCommit=false.

    Directory *lastDir = directory;
    string lastDocStoreSegment;
    int32_t next = -1;

    bool mergeDocStores = false;
    bool doFlushDocStore = false;
    const string currentDocStoreSegment = docWriter->getDocStoreSegment();

    // Test each segment to be merged: check if we need to
    // flush/merge doc stores
    for (int32_t i = 0; i < end; i++) {
        SegmentInfo *si = sourceSegments->info(i);

        // If it has deletions we must merge the doc stores
        if (si->hasDeletions())
            mergeDocStores = true;

        // If it has its own (private) doc stores we must
        // merge the doc stores
        if (-1 == si->getDocStoreOffset())
            mergeDocStores = true;

        // If it has a different doc store segment than
        // previous segments, we must merge the doc stores
        string docStoreSegment = si->getDocStoreSegment();
        if (docStoreSegment.empty())
            mergeDocStores = true;
        else if (lastDocStoreSegment.empty())
            lastDocStoreSegment = docStoreSegment;
        else if (!lastDocStoreSegment.compare(docStoreSegment) == 0)
            mergeDocStores = true;

        // Segments' docScoreOffsets must be in-order,
        // contiguous.  For the default merge policy now
        // this will always be the case but for an arbitrary
        // merge policy this may not be the case
        if (-1 == next)
            next = si->getDocStoreOffset() + si->docCount;
        else if (next != si->getDocStoreOffset())
            mergeDocStores = true;
        else
            next = si->getDocStoreOffset() + si->docCount;

        // If the segment comes from a different directory
        // we must merge
        if (lastDir != si->dir)
            mergeDocStores = true;

        // If the segment is referencing the current "live"
        // doc store outputs then we must merge
        if (si->getDocStoreOffset() != -1 && !currentDocStoreSegment.empty() && si->getDocStoreSegment().compare(currentDocStoreSegment) == 0)
            doFlushDocStore = true;
    }

    int32_t docStoreOffset;
    string docStoreSegment;
    bool docStoreIsCompoundFile;

    if (mergeDocStores) {
        docStoreOffset = -1;
        docStoreSegment.clear();
        docStoreIsCompoundFile = false;
    } else {
        SegmentInfo *si = sourceSegments->info(0);
        docStoreOffset = si->getDocStoreOffset();
        docStoreSegment = si->getDocStoreSegment();
        docStoreIsCompoundFile = si->getDocStoreIsCompoundFile();
    }

    if (mergeDocStores && doFlushDocStore) {
        // SegmentMerger intends to merge the doc stores
        // (stored fields, vectors), and at least one of the
        // segments to be merged refers to the currently
        // live doc stores.

        // TODO: if we know we are about to merge away these
        // newly flushed doc store files then we should not
        // make compound file out of them...
        if (infoStream != NULL)
            message(string("flush at merge"));
        flush(false, true);
    }

    // We must take a full copy at this point so that we can
    // properly merge deletes in commitMerge()
    _merge->segmentsClone = _merge->segments->clone();

    for (int32_t i = 0; i < end; i++) {
        SegmentInfo *si = _merge->segmentsClone->info(i);

        // IncRef all files for this segment info to make sure
        // they are not removed while we are trying to merge->
        if (si->dir == directory)
            deleter->incRef(si->files());
    }

    _merge->increfDone = true;

    _merge->mergeDocStores = mergeDocStores;

    // Bind a _CLNEW segment name here so even with
    // ConcurrentMergePolicy we keep deterministic segment
    // names.
    _merge->info = _CLNEW SegmentInfo(newSegmentName().c_str(), 0,
                                      directory, false, true,
                                      docStoreOffset,
                                      docStoreSegment.c_str(),
                                      docStoreIsCompoundFile);
    // Also enroll the merged segment into mergingSegments;
    // this prevents it from getting selected for a merge
    // after our merge is done but while we are building the
    // CFS:
    mergingSegments->insert(_merge->info);
}

void IndexWriter::mergeFinish(MergePolicy::OneMerge *_merge) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)

    if (_merge->increfDone)
        decrefMergeSegments(_merge);

    assert(_merge->registerDone);

    const SegmentInfos *sourceSegments = _merge->segments;
    const int32_t end = sourceSegments->size();
    for (int32_t i = 0; i < end; i++) {//todo: use iterator
        MergingSegmentsType::iterator itr = mergingSegments->find(sourceSegments->info(i));
        if (itr != mergingSegments->end()) mergingSegments->remove(itr);
    }
    MergingSegmentsType::iterator itr = mergingSegments->find(_merge->info);
    if (itr != mergingSegments->end()) mergingSegments->remove(itr);
    _merge->registerDone = false;
}

int32_t IndexWriter::mergeMiddle(MergePolicy::OneMerge *_merge) {

    _merge->checkAborted(directory);

    const string mergedName = _merge->info->name;

    int32_t mergedDocCount = 0;

    const SegmentInfos *sourceSegments = _merge->segments;
    SegmentInfos *sourceSegmentsClone = _merge->segmentsClone;
    const int32_t numSegments = sourceSegments->size();

    if (infoStream != NULL)
        message("merging " + _merge->segString(directory));

    SegmentMerger merger(this, mergedName.c_str(), _merge);

    // This is try/finally to make sure merger's readers are
    // closed:

    bool success = false;

    try {
        int32_t totDocCount = 0;

        for (int32_t i = 0; i < numSegments; i++) {
            SegmentInfo *si = sourceSegmentsClone->info(i);
            IndexReader *reader = SegmentReader::get(si, MERGE_READ_BUFFER_SIZE, _merge->mergeDocStores);// no need to set deleter (yet)
            merger.add(reader);
            totDocCount += reader->numDocs();
        }
        if (infoStream != NULL) {
            message(string("merge: total ") + Misc::toString(totDocCount) + " docs");
        }

        _merge->checkAborted(directory);

        mergedDocCount = _merge->info->docCount = merger.merge(_merge->mergeDocStores);

        assert(mergedDocCount == totDocCount);

        success = true;
    }
    _CLFINALLY(
            // close readers before we attempt to delete
            // now-obsolete segments
            merger.closeReaders();
            if (!success) {
                if (infoStream != NULL)
                    message("hit exception during merge; now refresh deleter on segment " + mergedName);
                {
                    SCOPED_LOCK_MUTEX(this->THIS_LOCK)
                    addMergeException(_merge);
                    deleter->refresh(mergedName.c_str());
                }
            })

    if (!commitMerge(_merge))
        // commitMerge will return false if this merge was aborted
        return 0;

    if (_merge->useCompoundFile) {

        success = false;
        bool skip = false;
        const string compoundFileName = mergedName + "." + IndexFileNames::COMPOUND_FILE_EXTENSION;

        try {
            try {
                merger.createCompoundFile(compoundFileName.c_str());
                success = true;
            } catch (CLuceneError &ioe) {
                if (ioe.number() != CL_ERR_IO) throw ioe;

                {
                    SCOPED_LOCK_MUTEX(this->THIS_LOCK)
                    if (segmentInfos->indexOf(_merge->info) == -1) {
                        // If another merge kicked in and merged our
                        // _CLNEW segment away while we were trying to
                        // build the compound file, we can hit a
                        // FileNotFoundException and possibly
                        // IOException over NFS.  We can tell this has
                        // happened because our SegmentInfo is no
                        // longer in the segments; if this has
                        // happened it is safe to ignore the exception
                        // & skip finishing/committing our compound
                        // file creating.
                        if (infoStream != NULL)
                            message("hit exception creating compound file; ignoring it because our info (segment " + _merge->info->name + ") has been merged away");
                        skip = true;
                    } else
                        throw ioe;
                }
            }
        }
        _CLFINALLY(
                if (!success) {
                    if (infoStream != NULL)
                        message(string("hit exception creating compound file during merge: skip=") + Misc::toString(skip));

                    {
                        SCOPED_LOCK_MUTEX(this->THIS_LOCK)
                        if (!skip)
                            addMergeException(_merge);
                        deleter->deleteFile(compoundFileName.c_str());
                    }
                })

        if (!skip) {

            {
                SCOPED_LOCK_MUTEX(this->THIS_LOCK)
                if (skip || segmentInfos->indexOf(_merge->info) == -1 || _merge->isAborted()) {
                    // Our segment (committed in non-compound
                    // format) got merged away while we were
                    // building the compound format.
                    deleter->deleteFile(compoundFileName.c_str());
                } else {
                    success = false;
                    try {
                        _merge->info->setUseCompoundFile(true);
                        checkpoint();
                        success = true;
                    }
                    _CLFINALLY(
                            if (!success) {
                                if (infoStream != NULL)
                                    message(string("hit exception checkpointing compound file during merge"));

                                // Must rollback:
                                addMergeException(_merge);
                                _merge->info->setUseCompoundFile(false);
                                deletePartialSegmentsFile();
                                deleter->deleteFile(compoundFileName.c_str());
                            })

                    // Give deleter a chance to remove files now.
                    deleter->checkpoint(segmentInfos, autoCommit);
                }
            }
        }
    }

    return mergedDocCount;
}

void IndexWriter::addMergeException(MergePolicy::OneMerge *_merge) {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    if (mergeGen == _merge->mergeGen) {
        MergeExceptionsType::iterator itr = mergeExceptions->begin();
        while (itr != mergeExceptions->end()) {
            MergePolicy::OneMerge *x = *itr;
            if (x == _merge) {
                return;
            }
        }
    }
    mergeExceptions->push_back(_merge);
}

void IndexWriter::deletePartialSegmentsFile() {
    if (segmentInfos->getLastGeneration() != segmentInfos->getGeneration()) {
        string segmentFileName = IndexFileNames::fileNameFromGeneration(IndexFileNames::SEGMENTS,
                                                                        "",
                                                                        segmentInfos->getGeneration());
        if (infoStream != NULL)
            message("now delete partial segments file \"" + segmentFileName + "\"");

        deleter->deleteFile(segmentFileName.c_str());
    }
}


void IndexWriter::applyDeletes(bool flushedNewSegment) {
    const TermNumMapType &bufferedDeleteTerms = docWriter->getBufferedDeleteTerms();
    const vector<int32_t> *bufferedDeleteDocIDs = docWriter->getBufferedDeleteDocIDs();

    if (infoStream != NULL)
        message(string("flush ") + Misc::toString(docWriter->getNumBufferedDeleteTerms()) +
                " buffered deleted terms and " + Misc::toString((int32_t) bufferedDeleteDocIDs->size()) +
                " deleted docIDs on " + Misc::toString((int32_t) segmentInfos->size()) + " segments.");

    if (flushedNewSegment) {
        IndexReader *reader = NULL;
        try {
            // Open readers w/o opening the stored fields /
            // vectors because these files may still be held
            // open for writing by docWriter
            reader = SegmentReader::get(segmentInfos->info(segmentInfos->size() - 1), false);

            // Apply delete terms to the segment just flushed from ram
            // apply appropriately so that a delete term is only applied to
            // the documents buffered before it, not those buffered after it.
            _internal->applyDeletesSelectively(bufferedDeleteTerms, *bufferedDeleteDocIDs, reader);
        }
        _CLFINALLY(
                if (reader != NULL) {
                    try {
                        reader->doCommit();
                    }
                    _CLFINALLY(
                            reader->doClose();
                            _CLLDELETE(reader);)
                })
    }

    int32_t infosEnd = segmentInfos->size();
    if (flushedNewSegment) {
        infosEnd--;
    }

    for (int32_t i = 0; i < infosEnd; i++) {
        IndexReader *reader = NULL;
        try {
            reader = SegmentReader::get(segmentInfos->info(i), false);

            // Apply delete terms to disk segments
            // except the one just flushed from ram.
            _internal->applyDeletes(bufferedDeleteTerms, reader);
        }
        _CLFINALLY(
                if (reader != NULL) {
                    try {
                        reader->doCommit();
                    }
                    _CLFINALLY(
                            reader->doClose();)
                })
    }

    // Clean up bufferedDeleteTerms.
    docWriter->clearBufferedDeletes();
}


int32_t IndexWriter::getBufferedDeleteTermsSize() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    return docWriter->getBufferedDeleteTerms().size();
}

int32_t IndexWriter::getNumBufferedDeleteTerms() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    return docWriter->getNumBufferedDeleteTerms();
}

void IndexWriter::Internal::applyDeletesSelectively(const TermNumMapType &deleteTerms,
                                                    const vector<int32_t> &deleteIds, IndexReader *reader) {
    TermNumMapType::const_iterator iter = deleteTerms.begin();
    while (iter != deleteTerms.end()) {
        Term *term = iter->first;
        TermDocs *docs = reader->termDocs(term);
        if (docs != NULL) {
            int32_t num = iter->second->getNum();
            try {
                while (docs->next()) {
                    int32_t doc = docs->doc();
                    if (doc >= num) {
                        break;
                    }
                    reader->deleteDocument(doc);
                }
            }
            _CLFINALLY(
                    docs->close();
                    _CLDELETE(docs);)
        }

        iter++;
    }

    if (deleteIds.size() > 0) {
        vector<int32_t>::const_iterator iter2 = deleteIds.begin();
        while (iter2 != deleteIds.end()) {
            reader->deleteDocument(*iter2);
            ++iter2;
        }
    }
}

void IndexWriter::Internal::applyDeletes(const TermNumMapType &deleteTerms, IndexReader *reader) {
    TermNumMapType::const_iterator iter = deleteTerms.begin();
    while (iter != deleteTerms.end()) {
        reader->deleteDocuments(iter->first);
        iter++;
    }
}

SegmentInfo *IndexWriter::newestSegment() {
    return segmentInfos->info(segmentInfos->size() - 1);
}

string IndexWriter::segString() {
    SCOPED_LOCK_MUTEX(THIS_LOCK)
    std::string buffer;
    for (int32_t i = 0; i < segmentInfos->size(); i++) {
        if (i > 0) {
            buffer += " ";
        }
        buffer += segmentInfos->info(i)->segString(directory);
    }

    return buffer;
}

bool IndexWriter::testPoint(const char *name) {
    return true;
}

CL_NS_END

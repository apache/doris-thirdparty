// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License

#include <cstdint>
#include <iostream>
#include <utility>
#include <vector>
#include "CLucene/debug/mem.h"
#include "test.h"
#include "CLucene/debug/error.h"
#include "CLucene/index/IndexWriter.h"
#include "CLucene/store/IndexInput.h"
#include "CLucene/store/IndexOutput.h"
#include "roaring/roaring.hh"

void _setupSourceNullBitmapValues(std::vector<std::vector<uint32_t>> &srcNullBitmapValues) {
    srcNullBitmapValues.push_back(std::vector<uint32_t>{1, 2, 3});
    srcNullBitmapValues.push_back(std::vector<uint32_t>{2, 3, 4});
    srcNullBitmapValues.push_back(std::vector<uint32_t>{3, 4, 5});
}

void _setupTransVec(std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& trans_vec) {

    trans_vec.resize(3);
    for (int i = 0; i < 3; i++) {
        trans_vec[i].resize(6);
    }
    
    trans_vec[0][0] = std::pair<uint32_t, uint32_t>{0, 1};
    trans_vec[0][1] = std::pair<uint32_t, uint32_t>{0, 2};
    trans_vec[0][2] = std::pair<uint32_t, uint32_t>{0, 5};
    trans_vec[0][3] = std::pair<uint32_t, uint32_t>{0, 7};
    trans_vec[0][4] = std::pair<uint32_t, uint32_t>{0, 3};
    trans_vec[0][5] = std::pair<uint32_t, uint32_t>{0, 8};
    trans_vec[1][0] = std::pair<uint32_t, uint32_t>{0, 4};
    trans_vec[1][1] = std::pair<uint32_t, uint32_t>{0, 6};
    trans_vec[1][2] = std::pair<uint32_t, uint32_t>{UINT32_MAX, UINT32_MAX};
    trans_vec[1][3] = std::pair<uint32_t, uint32_t>{1, 1};
    trans_vec[1][4] = std::pair<uint32_t, uint32_t>{1, 2};
    trans_vec[1][5] = std::pair<uint32_t, uint32_t>{1, 9};
    trans_vec[2][0] = std::pair<uint32_t, uint32_t>{1, 3};
    trans_vec[2][1] = std::pair<uint32_t, uint32_t>{1, 4};
    trans_vec[2][2] = std::pair<uint32_t, uint32_t>{1, 5};
    trans_vec[2][3] = std::pair<uint32_t, uint32_t>{1, 6};
    trans_vec[2][4] = std::pair<uint32_t, uint32_t>{1, 7};
    trans_vec[2][5] = std::pair<uint32_t, uint32_t>{1, 8};
}

uint64_t _getNullBitmapCardinality(RAMDirectory& dir) {
    IndexInput* null_bitmap_in = nullptr;
    CLuceneError error;
    dir.openInput(IndexWriter::NULL_BITMAP_FILE_NAME, null_bitmap_in, error);
    if (error.number() != 0) {
        return 0;
    }
    size_t null_bitmap_size = null_bitmap_in->length();
    std::string buf;
    buf.resize(null_bitmap_size);
    null_bitmap_in->readBytes(reinterpret_cast<uint8_t*>(const_cast<char*>(buf.data())), null_bitmap_size);
    auto null_bitmap = roaring::Roaring::read(buf.data(), false);
    null_bitmap.runOptimize();

    // close resources
    null_bitmap_in->close();
    _CLLDELETE(null_bitmap_in);

    return null_bitmap.cardinality();
}

// src segments -> dest segments
//           3  -> 2
// docs      18 -> 17
// 1,2,3,4,5,6
// 1,2,3,4,5,6  -> 1,2,3,4,5,6,7,8 
// 1,2,3,4,5,6     1,2,3,4,5,6,7,8,9
//
// null values
// 1,2,3
// 2,3,4        -> 2,5,7
// 3,4,5           1,2,6,7,8
void TestMergeNullBitmapWriteNullBitmap(CuTest *tc) {
    lucene::analysis::SimpleAnalyzer<char> analyzer;
    RAMDirectory dir;
    auto* index_writer = _CLNEW lucene::index::IndexWriter(&dir, &analyzer, true);
    std::vector<std::vector<uint32_t>> srcNullBitmapValues;
    std::vector<std::shared_ptr<lucene::store::IndexOutput>> nullBitmapIndexOutputList;

    _setupSourceNullBitmapValues(srcNullBitmapValues);

    // setup transVec
    // translation vec
    // <<dest_idx_num, dest_docId>>
    // the first level vector: index indicates src segment.
    // the second level vector: index indicates row id of source segment,
    // value indicates row id of destination segment.
    // <UINT32_MAX, UINT32_MAX> indicates current row not exist.
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec;
    _setupTransVec(trans_vec);

    RAMDirectory dest_dir1;
    RAMDirectory dest_dir2;
    {
        auto dest_output_index1 = std::shared_ptr<lucene::store::IndexOutput>(dest_dir1.createOutput(IndexWriter::NULL_BITMAP_FILE_NAME), ResourceDeleter<lucene::store::IndexOutput>());
        auto dest_output_index2 = std::shared_ptr<lucene::store::IndexOutput>(dest_dir2.createOutput(IndexWriter::NULL_BITMAP_FILE_NAME), ResourceDeleter<lucene::store::IndexOutput>());
        nullBitmapIndexOutputList.push_back(dest_output_index1);
        nullBitmapIndexOutputList.push_back(dest_output_index2);

        try {
            index_writer->setNumDestIndexes(2);
            index_writer->setTransVec(std::make_shared<const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>>(trans_vec));
            index_writer->setNullBitmapIndexOutputList(nullBitmapIndexOutputList);
            index_writer->mergeNullBitmap(srcNullBitmapValues);
        } catch (const std::exception& ex) {
            std::cout << "Caught exception: " << ex.what() << std::endl;
        } catch (...) {
            std::cout << "merge null bitmap failed" << std::endl;
            return;
        }
        nullBitmapIndexOutputList.clear();
        index_writer->close();
        _CLDELETE(index_writer);
    }

    // check cardinality
    uint64_t source_cardinality = 0;
    for (const auto& vec : srcNullBitmapValues) {
        source_cardinality += vec.size();
    }
    auto dest_cardinality1 = _getNullBitmapCardinality(dest_dir1);
    auto dest_cardinality2 = _getNullBitmapCardinality(dest_dir2);
    auto dest_cardinality = dest_cardinality1 + dest_cardinality2;
    
    // 9 = 8 + 1
    CLUCENE_ASSERT(source_cardinality == (dest_cardinality + 1));
    
    // release resources
    dest_dir1.close();
    dest_dir2.close();
    dir.close();
}

void TestMergeNullBitmapEmptySrc(CuTest *tc) {
    lucene::analysis::SimpleAnalyzer<char> analyzer;
    RAMDirectory dir;
    auto* index_writer = _CLNEW lucene::index::IndexWriter(&dir, &analyzer, true);
    // empty source bitmap values
    std::vector<std::vector<uint32_t>> srcNullBitmapValues;
    std::vector<std::shared_ptr<lucene::store::IndexOutput>> nullBitmapIndexOutputList;

    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec;
    _setupTransVec(trans_vec);

    RAMDirectory dest_dir1;
    RAMDirectory dest_dir2;
    {
        auto dest_output_index1 = std::shared_ptr<lucene::store::IndexOutput>(dest_dir1.createOutput(IndexWriter::NULL_BITMAP_FILE_NAME), ResourceDeleter<lucene::store::IndexOutput>());
        auto dest_output_index2 = std::shared_ptr<lucene::store::IndexOutput>(dest_dir2.createOutput(IndexWriter::NULL_BITMAP_FILE_NAME), ResourceDeleter<lucene::store::IndexOutput>());
        nullBitmapIndexOutputList.push_back(dest_output_index1);
        nullBitmapIndexOutputList.push_back(dest_output_index2);

        try {
            index_writer->setNumDestIndexes(2);
            index_writer->setTransVec(std::make_shared<const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>>(trans_vec));
            index_writer->setNullBitmapIndexOutputList(nullBitmapIndexOutputList);
            index_writer->mergeNullBitmap(srcNullBitmapValues);
        } catch (const std::exception& ex) {
            std::cout << "Caught exception: " << ex.what() << std::endl;
        } catch (...) {
            std::cout << "merge null bitmap failed" << std::endl;
            return;
        }
        nullBitmapIndexOutputList.clear();
        index_writer->close();
        _CLDELETE(index_writer);
    }

    // check cardinality
    uint64_t source_cardinality = 0;
    for (const auto& vec : srcNullBitmapValues) {
        source_cardinality += vec.size();
    }
    auto dest_cardinality1 = _getNullBitmapCardinality(dest_dir1);
    auto dest_cardinality2 = _getNullBitmapCardinality(dest_dir2);
    auto dest_cardinality = dest_cardinality1 + dest_cardinality2;
    
    // 0 = 0
    CLUCENE_ASSERT(source_cardinality == dest_cardinality);
    
    // release resources
    dest_dir1.close();
    dest_dir2.close();
    dir.close();
}

void TestMergeNullBitmapEmptyIndexSrcBitmapValues(CuTest *tc) {
    lucene::analysis::SimpleAnalyzer<char> analyzer;
    RAMDirectory dir;
    auto* index_writer = _CLNEW lucene::index::IndexWriter(&dir, &analyzer, true);
    // empty source bitmap values for every index
    std::vector<std::vector<uint32_t>> srcNullBitmapValues;
    srcNullBitmapValues.push_back(std::vector<uint32_t>());
    srcNullBitmapValues.push_back(std::vector<uint32_t>());
    srcNullBitmapValues.push_back(std::vector<uint32_t>());

    std::vector<std::shared_ptr<lucene::store::IndexOutput>> nullBitmapIndexOutputList;

    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec;
    _setupTransVec(trans_vec);

    RAMDirectory dest_dir1;
    RAMDirectory dest_dir2;
    {
        auto dest_output_index1 = std::shared_ptr<lucene::store::IndexOutput>(dest_dir1.createOutput(IndexWriter::NULL_BITMAP_FILE_NAME), ResourceDeleter<lucene::store::IndexOutput>());
        auto dest_output_index2 = std::shared_ptr<lucene::store::IndexOutput>(dest_dir2.createOutput(IndexWriter::NULL_BITMAP_FILE_NAME), ResourceDeleter<lucene::store::IndexOutput>());
        nullBitmapIndexOutputList.push_back(dest_output_index1);
        nullBitmapIndexOutputList.push_back(dest_output_index2);

        try {
            index_writer->setNumDestIndexes(2);
            index_writer->setTransVec(std::make_shared<const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>>(trans_vec));
            index_writer->setNullBitmapIndexOutputList(nullBitmapIndexOutputList);
            index_writer->mergeNullBitmap(srcNullBitmapValues);
        } catch (const std::exception& ex) {
            std::cout << "Caught exception: " << ex.what() << std::endl;
        } catch (...) {
            std::cout << "merge null bitmap failed" << std::endl;
            return;
        }
        nullBitmapIndexOutputList.clear();
        index_writer->close();
        _CLDELETE(index_writer);
    }

    // check cardinality
    uint64_t source_cardinality = 0;
    for (const auto& vec : srcNullBitmapValues) {
        source_cardinality += vec.size();
    }
    auto dest_cardinality1 = _getNullBitmapCardinality(dest_dir1);
    auto dest_cardinality2 = _getNullBitmapCardinality(dest_dir2);
    auto dest_cardinality = dest_cardinality1 + dest_cardinality2;
    
    // 0 = 0
    CLUCENE_ASSERT(source_cardinality == dest_cardinality);
    
    // release resources
    dest_dir1.close();
    dest_dir2.close();
    dir.close();
}

void TestMergeNullBitmapIgnoreDoc(CuTest *tc) {
    lucene::analysis::SimpleAnalyzer<char> analyzer;
    RAMDirectory dir;
    auto* index_writer = _CLNEW lucene::index::IndexWriter(&dir, &analyzer, true);
    std::vector<std::vector<uint32_t>> srcNullBitmapValues;
    _setupSourceNullBitmapValues(srcNullBitmapValues);

    std::vector<std::shared_ptr<lucene::store::IndexOutput>> nullBitmapIndexOutputList;

    // all docs in src index are ignored
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec;
    trans_vec.resize(srcNullBitmapValues.size());
    for (int i = 0; i < trans_vec.size(); i++) {
        trans_vec[i].resize(6);
    }
    for (int i = 0; i < srcNullBitmapValues.size(); i++) {
        for (int j = 0; j < 6; j++) {
            trans_vec[i][j] = std::pair<uint32_t, uint32_t>{UINT32_MAX, UINT32_MAX};
        }
    }

    RAMDirectory dest_dir1;
    RAMDirectory dest_dir2;
    {
        auto dest_output_index1 = std::shared_ptr<lucene::store::IndexOutput>(dest_dir1.createOutput(IndexWriter::NULL_BITMAP_FILE_NAME), ResourceDeleter<lucene::store::IndexOutput>());
        auto dest_output_index2 = std::shared_ptr<lucene::store::IndexOutput>(dest_dir2.createOutput(IndexWriter::NULL_BITMAP_FILE_NAME), ResourceDeleter<lucene::store::IndexOutput>());
        nullBitmapIndexOutputList.push_back(dest_output_index1);
        nullBitmapIndexOutputList.push_back(dest_output_index2);

        try {
            index_writer->setNumDestIndexes(2);
            index_writer->setTransVec(std::make_shared<const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>>(trans_vec));
            index_writer->setNullBitmapIndexOutputList(nullBitmapIndexOutputList);
            index_writer->mergeNullBitmap(srcNullBitmapValues);
        } catch (const std::exception& ex) {
            std::cout << "Caught exception: " << ex.what() << std::endl;
        } catch (...) {
            std::cout << "merge null bitmap failed" << std::endl;
            return;
        }
        nullBitmapIndexOutputList.clear();
        index_writer->close();
        _CLDELETE(index_writer);
    }

    // check cardinality
    uint64_t source_cardinality = 0;
    for (const auto& vec : srcNullBitmapValues) {
        source_cardinality += vec.size();
    }
    auto dest_cardinality1 = _getNullBitmapCardinality(dest_dir1);
    auto dest_cardinality2 = _getNullBitmapCardinality(dest_dir2);
    auto dest_cardinality = dest_cardinality1 + dest_cardinality2;

    // 9 = 0 + 9
    CLUCENE_ASSERT(source_cardinality == dest_cardinality + source_cardinality);
    
    // release resources
    dest_dir1.close();
    dest_dir2.close();
    dir.close();
}



CuSuite* testIndexCompaction() {
    CuSuite* suite = CuSuiteNew(_T("CLucene Index Compaction Test"));

    SUITE_ADD_TEST(suite, TestMergeNullBitmapWriteNullBitmap);
    SUITE_ADD_TEST(suite, TestMergeNullBitmapEmptySrc);
    SUITE_ADD_TEST(suite, TestMergeNullBitmapEmptyIndexSrcBitmapValues);
    SUITE_ADD_TEST(suite, TestMergeNullBitmapIgnoreDoc);

    return suite;
}
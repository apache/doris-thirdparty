PROJECT(clucene-contribs-lib)

INCLUDE (DefineOptions)
DEFINE_OPTIONS(EXTRA_OPTIONS EXTRA_LIBS)
ADD_DEFINITIONS(${EXTRA_OPTIONS} -DMAKE_CLUCENE_CONTRIBS_LIB)

set(CMAKE_MODULE_PATH "${clucene-contribs-lib_SOURCE_DIR}/cmake")

option(ENABLE_CLANG_COVERAGE "coverage option" OFF)
if (ENABLE_CLANG_COVERAGE AND ENABLE_CLANG_COVERAGE STREQUAL ON AND COMPILER_CLANG)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
endif ()

#add the files to our groups and core
SOURCE_GROUP("analysis" ./CLucene/analysis/*)
SOURCE_GROUP("util" ./CLucene/util/*)
#seems to be a bug in cmake, can't put these in analysis\\standard folder :(
SOURCE_GROUP("analysis-cjk" ./CLucene/analysis/cjk/*)
SOURCE_GROUP("analysis-jieba" ./CLucene/analysis/jieba/*)
SOURCE_GROUP("analysis-de" ./CLucene/analysis/de/*)
SOURCE_GROUP("analysis-ik" ./CLucene/analysis/ik/*)
#SOURCE_GROUP("highlighter" ./CLucene/highlighter/*)
SOURCE_GROUP("index" ./CLucene/index/*)
SOURCE_GROUP("snowball" ./CLucene/snowball/*)
SOURCE_GROUP("snowball-impl" ./CLucene/snowball/include/*)
SOURCE_GROUP("snowball-impl" ./CLucene/snowball/libstemmer/*)
SOURCE_GROUP("snowball-impl" ./CLucene/snowball/runtime/*)
SOURCE_GROUP("snowball-impl" ./CLucene/snowball/src_c/*)

SET(clucene_contribs_Files
    ./CLucene/util/gzipcompressstream.cpp
    ./CLucene/util/gzipinputstream.cpp

	./CLucene/analysis/LanguageBasedAnalyzer.cpp
	./CLucene/analysis/cjk/CJKAnalyzer.cpp

	./CLucene/analysis/de/GermanAnalyzer.cpp
	./CLucene/analysis/de/GermanStemFilter.cpp
	./CLucene/analysis/de/GermanStemmer.cpp

	#./CLucene/highlighter/Encoder.cpp
	#./CLucene/highlighter/Formatter.cpp
	#./CLucene/highlighter/Fragmenter.cpp
	#./CLucene/highlighter/Highlighter.cpp
	#./CLucene/highlighter/QueryScorer.cpp
	#./CLucene/highlighter/QueryTermExtractor.cpp
	#./CLucene/highlighter/SimpleFragmenter.cpp
	#./CLucene/highlighter/SimpleHTMLEncoder.cpp
	#./CLucene/highlighter/SimpleHTMLFormatter.cpp
	#./CLucene/highlighter/TextFragment.cpp
	#./CLucene/highlighter/TokenGroup.cpp
	#./CLucene/highlighter/TokenSources.cpp
	#./CLucene/highlighter/WeightedTerm.cpp

	./CLucene/snowball/Snowball.cpp
	./CLucene/snowball/libstemmer/libstemmer.c
	./CLucene/snowball/runtime/api.c
	./CLucene/snowball/runtime/utilities.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_danish.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_dutch.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_english.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_finnish.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_french.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_german.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_italian.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_norwegian.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_porter.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_portuguese.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_spanish.c
	./CLucene/snowball/src_c/stem_ISO_8859_1_swedish.c
	./CLucene/snowball/src_c/stem_KOI8_R_russian.c
	./CLucene/snowball/src_c/stem_UTF_8_danish.c
	./CLucene/snowball/src_c/stem_UTF_8_dutch.c
	./CLucene/snowball/src_c/stem_UTF_8_english.c
	./CLucene/snowball/src_c/stem_UTF_8_finnish.c
	./CLucene/snowball/src_c/stem_UTF_8_french.c
	./CLucene/snowball/src_c/stem_UTF_8_german.c
	./CLucene/snowball/src_c/stem_UTF_8_italian.c
	./CLucene/snowball/src_c/stem_UTF_8_norwegian.c
	./CLucene/snowball/src_c/stem_UTF_8_porter.c
	./CLucene/snowball/src_c/stem_UTF_8_portuguese.c
	./CLucene/snowball/src_c/stem_UTF_8_russian.c
	./CLucene/snowball/src_c/stem_UTF_8_spanish.c
	./CLucene/snowball/src_c/stem_UTF_8_swedish.c
	./CLucene/analysis/jieba/ChineseTokenizer.cpp
	./CLucene/analysis/ik/dic/Dictionary.cpp
	./CLucene/analysis/ik/dic/DictSegment.cpp
	./CLucene/analysis/ik/IKTokenizer.cpp
	./CLucene/analysis/ik/core/Lexeme.cpp
	./CLucene/analysis/ik/core/QuickSortSet.cpp
	./CLucene/analysis/ik/core/LexemePath.cpp
	./CLucene/analysis/ik/core/IKSegmenter.cpp
	./CLucene/analysis/ik/core/CharacterUtil.cpp
	./CLucene/analysis/ik/core/AnalyzeContext.cpp
	./CLucene/analysis/ik/core/CJKSegmenter.cpp
	./CLucene/analysis/ik/core/IKArbitrator.cpp
	./CLucene/analysis/ik/core/LetterSegmenter.cpp
	./CLucene/analysis/ik/core/CN_QuantifierSegmenter.cpp
)
IF (BUILD_STATIC_LIBRARIES)
	SET ( clucene_contrib_extra_libs clucene-core-static clucene-shared-static ${EXTRA_LIBS})
ELSE()
	SET ( clucene_contrib_extra_libs clucene-core clucene-shared ${EXTRA_LIBS})
ENDIF()

#find our headers
file(GLOB_RECURSE HEADERS ${clucene-contribs-lib_SOURCE_DIR}/*.h)

#add extra capabilities
#find_package(ZLIB)
#IF ( NOT ZLIB_FOUND )
#  MESSAGE ( FATAL "ZLib not found" )
#ENDIF ( NOT ZLIB_FOUND )
#INCLUDE_DIRECTORIES( ${ZLIB_INCLUDE_DIR} )
#SET ( clucene_contrib_extra_libs "${clucene_contrib_extra_libs}" ${ZLIB_LIBRARIES} )

find_package(Iconv)
#find_package(Strigi)
SET ( clucene-contrib-libs "" )

IF ( USE_SHARED_OBJECT_FILES )
    GET_SHARED_FILES(clucene_shared_Files)
ENDIF ( USE_SHARED_OBJECT_FILES )

#create the libraries
INCLUDE_DIRECTORIES( ${clucene_SOURCE_DIR}/src/core )
INCLUDE_DIRECTORIES( ${clucene_SOURCE_DIR}/src/contribs-lib )

add_library(clucene-contribs-lib STATIC
	${clucene_contribs_Files} ${clucene_shared_Files} ${HEADERS}
)
TARGET_LINK_LIBRARIES(clucene-contribs-lib ${clucene_contrib_extra_libs})

#set properties on the libraries
SET_TARGET_PROPERTIES(clucene-contribs-lib PROPERTIES
    VERSION ${CLUCENE_VERSION}
    SOVERSION ${CLUCENE_SOVERSION}
    COMPILE_DEFINITIONS_DEBUG _DEBUG
)

install(TARGETS clucene-contribs-lib
		DESTINATION ${LIB_DESTINATION}
		COMPONENT development )

install(FILES ${clucene_SOURCE_DIR}/src/contribs-lib/CLucene/analysis/cjk/CJKAnalyzer.h
		DESTINATION include/CLucene/analysis/cjk
		COMPONENT development )

install(FILES ${clucene_SOURCE_DIR}/src/contribs-lib/CLucene/analysis/LanguageBasedAnalyzer.h
		DESTINATION include/CLucene/analysis
		COMPONENT development )

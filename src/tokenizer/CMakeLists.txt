PROJECT(clucene-mmseg)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clucene-mmseg include)
add_headers_and_sources(clucene-mmseg src)

add_library(clucene-mmseg-static STATIC ${clucene-mmseg_headers} ${clucene-mmseg_sources})
target_include_directories (clucene-mmseg-static PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

#original make
#set(TOKENIZER_DIR ${CMAKE_CURRENT_SOURCE_DIR})

#include_directories(
#	${TOKENIZER_DIR}
#	${TOKENIZER_DIR}/include
#)

#file(GLOB TOKENIZER_SRC_PATH ./src/*.cpp ./src/*.c)

#add_library(tokenizer STATIC ${TOKENIZER_SRC_PATH})

#target_link_libraries(tokenizer)
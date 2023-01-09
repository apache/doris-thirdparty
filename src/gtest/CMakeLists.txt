include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)

file(GLOB_RECURSE TESTER_SOURCES
    "parser/*.cpp"
)

#set(PLATFORM_LIBS ${CMAKE_DL_LIBS})

GET_SHARED_FILES(clucene_shared_Files)

#message (STATUS "Parser Using  ${ZLIB_LIBRARIES}: ${ZLIB_INCLUDE_DIR}")
#message (STATUS "Parser Using  shared files: ${clucene_shared_Files}")

add_executable(unittest_parser EXCLUDE_FROM_ALL ${clucene_shared_Files} ${TESTER_SOURCES})
target_link_libraries(unittest_parser PRIVATE ${ZLIB_LIBRARIES} clucene-core-static clucene-shared-static ${GTEST_LIBRARIES}  )
target_include_directories(unittest_parser
        PUBLIC
        ${ZLIB_INCLUDE_DIR}
        #${ClickHouse_SOURCE_DIR}/contrib/zlib-ng
        ${ClickHouse_SOURCE_DIR}/clucene/src/shared
        ${ClickHouse_SOURCE_DIR}/clucene/src/core
        ${CMAKE_CURRENT_BINARY_DIR}/../clucene/src/shared  #for _clucene-config.h
        )


add_check(unittest_parser)
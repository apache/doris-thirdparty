PROJECT(cl_demo)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" "${clucene_SOURCE_DIR}/cmake" "${clucene-shared_SOURCE_DIR}/cmake")

INCLUDE (DefineOptions)
DEFINE_OPTIONS(EXTRA_OPTIONS EXTRA_LIBS)
ADD_DEFINITIONS(${EXTRA_OPTIONS})

INCLUDE_DIRECTORIES( ${clucene-demo_SOURCE_DIR} )
INCLUDE_DIRECTORIES( ${clucene_SOURCE_DIR}/src/core )

file(GLOB_RECURSE demo_HEADERS ${CMAKE_SOURCE_DIR}/test/*.h)

ADD_EXECUTABLE(cl_demo EXCLUDE_FROM_ALL
#./DeleteFiles.cpp
./IndexFiles.cpp
./Main.cpp
./SearchFiles.cpp
./Statistics.cpp
${clucene-shared_SOURCE_DIR}/CLucene/util/Misc.cpp
${clucene-shared_SOURCE_DIR}/CLucene/util/dirent.cpp
${demo_HEADERS}
)

ADD_EXECUTABLE(cl_demo_index EXCLUDE_FROM_ALL
        #./DeleteFiles.cpp
        ./IndexFiles.cpp
        ./Main_Index.cpp
        #./SearchFiles.cpp
        ./Statistics.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/Misc.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/dirent.cpp
        ${demo_HEADERS}
        )

#ADD_EXECUTABLE(cl_demo EXCLUDE_FROM_ALL
#./TestAnalyzer.cpp
#${demo_HEADERS}
#)
#add_library(ic STATIC IMPORTED)

#set_target_properties(ic PROPERTIES IMPORTED_LOCATION "${clucene-ext_SOURCE_DIR}/for/libic.a")

TARGET_LINK_LIBRARIES(cl_demo clucene-core-static clucene-shared-static zstd ic ${EXTRA_LIBS})
TARGET_LINK_LIBRARIES(cl_demo_index clucene-core-static clucene-shared-static zstd ic ${EXTRA_LIBS})

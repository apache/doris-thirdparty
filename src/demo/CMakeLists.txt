PROJECT(cl_demo)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake" "${clucene_SOURCE_DIR}/cmake" "${clucene-shared_SOURCE_DIR}/cmake")

INCLUDE (DefineOptions)
DEFINE_OPTIONS(EXTRA_OPTIONS EXTRA_LIBS)
ADD_DEFINITIONS(${EXTRA_OPTIONS})

INCLUDE_DIRECTORIES( ${clucene-demo_SOURCE_DIR} )
INCLUDE_DIRECTORIES( ${clucene_SOURCE_DIR}/src/core )
INCLUDE_DIRECTORIES( ${clucene_SOURCE_DIR}/src/contribs-lib )

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

TARGET_LINK_LIBRARIES(cl_demo clucene-core-static clucene-shared-static  ic ${EXTRA_LIBS})
TARGET_LINK_LIBRARIES(cl_demo_index clucene-core-static clucene-shared-static  ic ${EXTRA_LIBS})

ADD_EXECUTABLE(allIndex EXCLUDE_FROM_ALL
        ./allIndex.cpp
        ./simdjson.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/Misc.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/dirent.cpp
        ${demo_HEADERS}
)
TARGET_LINK_LIBRARIES(allIndex clucene-core-static clucene-shared-static clucene-contribs-lib ic ${EXTRA_LIBS})

ADD_EXECUTABLE(abnormal EXCLUDE_FROM_ALL
        ./abnormal.cpp
        ./simdjson.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/Misc.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/dirent.cpp
        ${demo_HEADERS}
)
TARGET_LINK_LIBRARIES(abnormal clucene-core-static clucene-shared-static clucene-contribs-lib ic ${EXTRA_LIBS})

ADD_EXECUTABLE(search EXCLUDE_FROM_ALL
        ./search.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/Misc.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/dirent.cpp
        ${demo_HEADERS}
)
TARGET_LINK_LIBRARIES(search clucene-core-static clucene-shared-static clucene-contribs-lib ic ${EXTRA_LIBS})

ADD_EXECUTABLE(strSearch EXCLUDE_FROM_ALL
        ./strSearch.cpp
        ./simdjson.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/Misc.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/dirent.cpp
        ${demo_HEADERS}
)
TARGET_LINK_LIBRARIES(strSearch clucene-core-static clucene-shared-static clucene-contribs-lib ic ${EXTRA_LIBS})

ADD_EXECUTABLE(pointSearch4 EXCLUDE_FROM_ALL
        ./pointSearch4.cpp
        ./simdjson.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/Misc.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/dirent.cpp
        ${demo_HEADERS}
)
TARGET_LINK_LIBRARIES(pointSearch4 clucene-core-static clucene-shared-static clucene-contribs-lib ic ${EXTRA_LIBS})

ADD_EXECUTABLE(pointSearch8 EXCLUDE_FROM_ALL
        ./pointSearch8.cpp
        ./simdjson.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/Misc.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/dirent.cpp
        ${demo_HEADERS}
)
TARGET_LINK_LIBRARIES(pointSearch8 clucene-core-static clucene-shared-static clucene-contribs-lib ic ${EXTRA_LIBS})

ADD_EXECUTABLE(demo EXCLUDE_FROM_ALL
        ./demo.cpp
        ./simdjson.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/Misc.cpp
        ${clucene-shared_SOURCE_DIR}/CLucene/util/dirent.cpp
        ${demo_HEADERS}
)
TARGET_LINK_LIBRARIES(demo clucene-core-static clucene-shared-static clucene-contribs-lib ic ${EXTRA_LIBS})
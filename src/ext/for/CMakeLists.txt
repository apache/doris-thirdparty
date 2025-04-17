cmake_minimum_required(VERSION 3.10)
project(powturbo)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_STANDARD 99)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(DEBUG "-DNDEBUG -g -O3")
set(OPT "-w -Wall -fstrict-aliasing -falign-loops -Wno-int-conversion")

if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
    set(SSE "-msse4.2")
    set(AVX2 "-mavx2")
    set(CMAKE_C_FLAGS ${SSE})
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    set(SSE "-march=armv8-a")
    set(CMAKE_C_FLAGS "-march=armv8-a")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc64le")
    set(SSE "-D__SSSE3__")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcpu=power9 -mtune=power9")
endif()

if(FLOAT16)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DUSE_FLOAT16")
endif()

if(STATIC)
    set(CMAKE_EXE_LINKER_FLAGS "-static")
endif()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${DEBUG} ${OPT}")
       
separate_arguments(avx2_c_flags_list UNIX_COMMAND "${DEBUG} ${OPT}")
separate_arguments(c_flags_list UNIX_COMMAND "${CMAKE_C_FLAGS}")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set(SRC_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/bitpack.c
        ${CMAKE_CURRENT_SOURCE_DIR}/bitunpack.c
        ${CMAKE_CURRENT_SOURCE_DIR}/vp4c.c
        ${CMAKE_CURRENT_SOURCE_DIR}/vp4d.c
        ${CMAKE_CURRENT_SOURCE_DIR}/transpose.c
        ${CMAKE_CURRENT_SOURCE_DIR}/bitutil.c
)

add_library(ic STATIC
    v8.c
    vint.c
    trlec.c
    trled.c
    vsimple.c 
    bitutil.c
    bitpack.c
    bitunpack.c
    vp4c.c
    vp4d.c
    transpose.c
)

foreach(SRC_FILE ${SRC_FILES})
    get_filename_component(SRC_NAME ${SRC_FILE} NAME_WE)
    set(SSE_OUTPUT ${SRC_NAME}_sse.o)
    set(AVX2_OUTPUT ${SRC_NAME}_avx2.o)

    add_custom_command(
        OUTPUT ${SSE_OUTPUT}
        COMMAND ${CMAKE_C_COMPILER} ${c_flags_list} -c -o ${SSE_OUTPUT} ${SRC_FILE}
        DEPENDS ${SRC_FILE}
    )

    target_sources(ic PRIVATE ${SSE_OUTPUT})

    if(USE_AVX2)
        add_custom_command(
            OUTPUT ${AVX2_OUTPUT}
	    COMMAND ${CMAKE_C_COMPILER} ${AVX2} -DAVX2_ON ${avx2_c_flags_list} -c -o ${AVX2_OUTPUT} ${SRC_FILE}
            DEPENDS ${SRC_FILE}
        )

        target_sources(ic PRIVATE ${AVX2_OUTPUT})
    endif()
endforeach()

add_executable(test_bitd1unpack test_bitd1unpack.cpp)

target_link_libraries(test_bitd1unpack ic)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
    target_compile_options(test_bitd1unpack PRIVATE ${AVX2} -DAVX2_ON ${DEBUG})
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    target_compile_options(test_bitd1unpack PRIVATE -march=armv8-a ${DEBUG})
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc64le")
    target_compile_options(test_bitd1unpack PRIVATE -mcpu=power9 -mtune=power9 -D__SSSE3__ ${DEBUG})
endif()

set(LIB_DESTINATION ../)

install(TARGETS ic
      DESTINATION ${LIB_DESTINATION}
      COMPONENT ext)

install(TARGETS test_bitd1unpack
      DESTINATION ${CMAKE_INSTALL_PREFIX}/bin
      COMPONENT tests)

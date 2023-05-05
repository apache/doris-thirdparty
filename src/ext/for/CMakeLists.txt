cmake_minimum_required(VERSION 3.10)
project(powturbo)

#INCLUDE (DefineOptions)
#DEFINE_OPTIONS(EXTRA_OPTIONS EXTRA_LIBS)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_STANDARD 99)

# Compiler options
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

#set(CMAKE_C_FLAGS_DEBUG "-DDEBUG -g")
set(CMAKE_C_FLAGS "-DNDEBUG -s -O3")
set(OPT "-w -Wall -fstrict-aliasing -falign-loops -Wno-int-conversion")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OPT}")


# Architecture-specific settings
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
    set(SSE "-msse4.2")
    set(AVX2 "-mavx2")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    set(SSE "-march=armv8-a")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc64le")
    set(SSE "-D__SSSE3__")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcpu=power9 -mtune=power9")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SSE}")

if(FLOAT16)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DUSE_FLOAT16")
endif()

if(STATIC)
    set(CMAKE_EXE_LINKER_FLAGS "-static")
endif()

separate_arguments(c_flags_list UNIX_COMMAND "${CMAKE_C_FLAGS}")
# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Define base source files
set(SRC_FILES
	${CMAKE_CURRENT_SOURCE_DIR}/bitpack.c
	${CMAKE_CURRENT_SOURCE_DIR}/bitunpack.c
	${CMAKE_CURRENT_SOURCE_DIR}/vp4c.c
	${CMAKE_CURRENT_SOURCE_DIR}/vp4d.c
	${CMAKE_CURRENT_SOURCE_DIR}/transpose.c
)

# Add base source files to library
add_library(ic STATIC
    bitutil.c
    fp.c
    v8.c
    vint.c
    trlec.c
    trled.c
    vsimple.c
    eliasfano.c
)

# Add custom commands to generate SSE and AVX2 versions of source files
foreach(SRC_FILE ${SRC_FILES})
    get_filename_component(SRC_NAME ${SRC_FILE} NAME_WE)
    set(SSE_OUTPUT ${SRC_NAME}_sse.o)
    set(AVX2_OUTPUT ${SRC_NAME}_avx2.o)

    add_custom_command(
        OUTPUT ${SSE_OUTPUT}
	COMMAND ${CMAKE_C_COMPILER} -c -o ${SSE_OUTPUT} ${SRC_FILE} ${c_flags_list} -DSSE2_ON ${SSE}
        DEPENDS ${SRC_FILE}
    )

    target_sources(ic PRIVATE ${SSE_OUTPUT})

    if(USE_AVX2)
        add_custom_command(
            OUTPUT ${AVX2_OUTPUT}
	    COMMAND ${CMAKE_C_COMPILER} -c -o ${AVX2_OUTPUT} ${SRC_FILE} ${c_flags_list} -DAVX2_ON ${AVX2}
            DEPENDS ${SRC_FILE}
        )

        target_sources(ic PRIVATE ${AVX2_OUTPUT})
    endif()
endforeach()
install(TARGETS ic
      DESTINATION ${LIB_DESTINATION}
      COMPONENT ext)

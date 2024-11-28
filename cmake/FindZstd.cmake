# FindZstd.cmake
# Try to find or build the zstd library

if(NOT TARGET zstd)
  message(STATUS "Zstd target not found, checking for local sources...")

  # Set up paths for Zstd
  set(ZSTD_DIR "${CMAKE_SOURCE_DIR}/3rdparty/zstd")
  set(ZSTD_INCLUDE_DIR "${ZSTD_DIR}/lib")
  set(ZSTD_BUILD_DIR "${CMAKE_BINARY_DIR}/zstd-build")
  set(ZSTD_LIBRARY "${ZSTD_BUILD_DIR}/lib/libzstd.a")

  # Ensure that the Zstd library and include directory are specified
  if(NOT EXISTS "${ZSTD_INCLUDE_DIR}/zstd.h" OR NOT EXISTS "${ZSTD_LIBRARY}")
    message(STATUS "Zstd not found or not built, building Zstd from downloaded sources...")

    # Configure and build Zstd
    file(MAKE_DIRECTORY "${ZSTD_BUILD_DIR}")
    execute_process(
      COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -S "${ZSTD_DIR}/build/cmake" -B "${ZSTD_BUILD_DIR}" -DCMAKE_INSTALL_PREFIX="" -DZSTD_BUILD_TESTS=OFF -DZSTD_BUILD_STATIC=ON -DZSTD_BUILD_PROGRAMS=OFF -DZSTD_BUILD_SHARED=OFF
      WORKING_DIRECTORY "${ZSTD_DIR}/build/cmake"
    )

    execute_process(
      COMMAND ${CMAKE_COMMAND} --build . --config Release
      WORKING_DIRECTORY "${ZSTD_BUILD_DIR}"
    )

    message(STATUS "ZSTD_INCLUDE_DIR: ${ZSTD_INCLUDE_DIR}")
    message(STATUS "ZSTD_LIBRARY: ${ZSTD_LIBRARY}")

    # After build, verify that the files exist
    if(NOT EXISTS "${ZSTD_INCLUDE_DIR}/zstd.h" OR NOT EXISTS "${ZSTD_LIBRARY}")
      message(FATAL_ERROR "Failed to build and locate Zstd")
    endif()
  else()
    message(STATUS "Found existing Zstd library at specified paths")
  endif()

  # Define the Zstd target
  add_library(zstd STATIC IMPORTED)
  set_target_properties(zstd PROPERTIES
    IMPORTED_LOCATION ${ZSTD_LIBRARY}
    INTERFACE_INCLUDE_DIRECTORIES ${ZSTD_INCLUDE_DIR}
  )

  include_directories(${ZSTD_INCLUDE_DIR})
else()
  message(STATUS "Zstd target already exists")
endif()
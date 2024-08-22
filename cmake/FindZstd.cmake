# FindZstd.cmake
# Try to find or build the zstd library

if(NOT TARGET zstd)
  message(STATUS "Zstd target not found, checking for local sources...")

  # Set up paths for Zstd
  set(ZSTD_VERSION "1.5.5")
  set(ZSTD_URL "https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz")
  set(ZSTD_TAR "${CMAKE_BINARY_DIR}/zstd-${ZSTD_VERSION}.tar.gz")
  set(ZSTD_DIR "${CMAKE_BINARY_DIR}/zstd-${ZSTD_VERSION}")
  set(ZSTD_INCLUDE_DIR "${ZSTD_DIR}/lib")
  set(ZSTD_LIBRARY "${CMAKE_BINARY_DIR}/zstd-install/lib64/libzstd.a")
  set(ZSTD_BUILD_DIR "${CMAKE_BINARY_DIR}/zstd-build")
  set(ZSTD_INSTALL_DIR "${CMAKE_BINARY_DIR}/zstd-install")

  # Check if the Zstd source directory exists
  if(NOT EXISTS "${ZSTD_DIR}")
    message(STATUS "Zstd directory not found, downloading Zstd v${ZSTD_VERSION} from ${ZSTD_URL}...")

    # Download the Zstd tarball
    file(DOWNLOAD ${ZSTD_URL} ${ZSTD_TAR} SHOW_PROGRESS)

    # Extract the tarball
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E tar xzf ${ZSTD_TAR}
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )
  endif()

  # Ensure that the Zstd library and include directory are specified
  if(NOT EXISTS "${ZSTD_INCLUDE_DIR}/zstd.h" OR NOT EXISTS "${ZSTD_LIBRARY}")
    message(STATUS "Zstd not found or not built, building Zstd from downloaded sources...")

    # Configure and build Zstd
    file(MAKE_DIRECTORY "${ZSTD_BUILD_DIR}")
    execute_process(
      COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -S "${ZSTD_DIR}/build/cmake" -B "${ZSTD_BUILD_DIR}" -DCMAKE_INSTALL_PREFIX=${ZSTD_INSTALL_DIR} -DZSTD_BUILD_TESTS=OFF -DZSTD_BUILD_STATIC=ON -DZSTD_BUILD_PROGRAMS=OFF -DZSTD_BUILD_SHARED=OFF
      WORKING_DIRECTORY "${ZSTD_DIR}/build/cmake"
    )

    execute_process(
      COMMAND ${CMAKE_COMMAND} --build . --target install --config Release
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
find_path(Roaring_INCLUDE_DIR
  NAMES roaring/roaring.h
  HINTS ${Roaring_ROOT}/include
)

find_library(Roaring_LIBRARY
  NAMES roaring
  HINTS ${Roaring_ROOT}/lib ${Roaring_ROOT}/lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Roaring
  REQUIRED_VARS Roaring_INCLUDE_DIR Roaring_LIBRARY
)

if(NOT Roaring_FOUND)
  message(STATUS "Roaring not found, trying to build from ext directory.")

  set(EXT_ROARING_DIR "${clucene-ext_SOURCE_DIR}")
  if(EXISTS "${EXT_ROARING_DIR}/roaring/roaring.c" AND EXISTS "${EXT_ROARING_DIR}/roaring/roaring.h" AND EXISTS "${EXT_ROARING_DIR}/roaring/roaring.hh")
    set(Roaring_INCLUDE_DIR ${EXT_ROARING_DIR})
    add_library(roaring STATIC "${EXT_ROARING_DIR}/roaring/roaring.c")
    #target_include_directories(roaring INTERFACE ${Roaring_INCLUDE_DIR})
    set(Roaring_LIBRARY roaring)
    set(Roaring_FOUND TRUE)
    message(STATUS "Roaring will be built from ext directory.")
  else()
    message(FATAL_ERROR "Roaring not found and ext directory does not contain required files.")
  endif()
endif()

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

if(Roaring_FOUND)
  set(Roaring_INCLUDE_DIRS ${Roaring_INCLUDE_DIR})
  set(Roaring_LIBRARIES ${Roaring_LIBRARY})
  mark_as_advanced(Roaring_INCLUDE_DIR Roaring_LIBRARY)
endif()


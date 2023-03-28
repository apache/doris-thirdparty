PROJECT(turbo-pfor)
set(PFOR_SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/ext/for)

add_custom_target(build_ic ALL
	COMMAND make OPT='-Wno-implicit-function-declaration -Wno-implicit-int -Wno-int-conversion' USE_AVX2=${USE_AVX2} libic.a -j 8
        WORKING_DIRECTORY ${PFOR_SOURCE_DIR}
        COMMENT "Original Turbo-PFOR makefile target")

add_library(ic STATIC IMPORTED)
set_target_properties(ic PROPERTIES IMPORTED_LOCATION "${PFOR_SOURCE_DIR}/libic.a")
add_dependencies(ic build_ic)

install(FILES ${PFOR_SOURCE_DIR}/libic.a 
	       DESTINATION "lib"
               COMPONENT development)

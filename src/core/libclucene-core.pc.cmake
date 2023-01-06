prefix=@CMAKE_INSTALL_PREFIX@
exec_prefix=${prefix}/bin
libdir=${prefix}/@LIB_DESTINATION@
includedir=${prefix}/include:${prefix}/include/CLucene/ext

Name: libclucene
Description: CLucene - a C++ search engine, ported from the popular Apache Lucene
Version: @CLUCENE_VERSION_MAJOR@.@CLUCENE_VERSION_MINOR@.@CLUCENE_VERSION_REVISION@.@CLUCENE_VERSION_PATCH@
Libs: -L${prefix}/@LIB_DESTINATION@/ -lclucene-core
Cflags: -I${prefix}/include -I${prefix}/include/CLucene/ext
~

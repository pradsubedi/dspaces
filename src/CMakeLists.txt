# list of source files
set(dspaces-src util.c bbox.c ss_data.c dspaces-client.c dspaces-server.c)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (dspaces-pkg "share/cmake/dspaces")

#
# library version set here (e.g. for shared libs).
#
set (dspaces_VERSION_MAJOR 0)
set (dspaces_VERSION_MINOR 1)
set (dspaces_VERSION_PATCH 0)
set (dspaces-vers "${dspaces_VERSION_MAJOR}.${dspaces_VERSION_MINOR}")
set (dspaces_VERSION "${dspaces-vers}.${dspaces_VERSION_PATCH}")

add_library(dspaces ${dspaces-src})
target_link_libraries (dspaces margo m pthread)
target_include_directories (dspaces PUBLIC $<INSTALL_INTERFACE:include>)

# local include's BEFORE, in case old incompatable .h files in prefix/include
target_include_directories (dspaces BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)

# for shared libs, establish the lib version
set_target_properties (dspaces
    PROPERTIES VERSION ${dspaces_VERSION}
    SOVERSION ${dspaces_VERSION_MAJOR})

#
# installation stuff (packaging and install commands)
#
write_basic_package_version_file(
    "dspaces-config-version.cmake"
    VERSION ${dspaces_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
configure_file (dspaces-config.cmake.in dspaces-config.cmake @ONLY)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (PRIVATE_LIBS "-ldspaces -lpthread")
configure_file ("dspaces.pc.in" "dspaces.pc" @ONLY)

#
# "make install" rules
#
install (TARGETS dspaces EXPORT dspaces-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (EXPORT dspaces-targets
         DESTINATION ${dspaces-pkg}
         FILE "dspaces-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/dspaces-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/dspaces-config-version.cmake"
               "../cmake/xpkg-import.cmake"
         DESTINATION ${dspaces-pkg} )
install (DIRECTORY ../include
         DESTINATION ${DEST_DIR}
         FILES_MATCHING PATTERN "*dspaces-*.h")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/dspaces.pc"
		DESTINATION "lib/pkgconfig/")

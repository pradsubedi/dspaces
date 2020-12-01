set(CMAKE_Fortran_MODULE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
include(FortranCInterface)

FortranCInterface_HEADER(FC.h MACRO_NAMESPACE "FC_")

add_library(dspaces_fortran
    dspaces_f2c.c
    dspaces_mod.f90
)

set_property(TARGET dspaces_fortran PROPERTY EXPORT_NAME fortran)
set_property(TARGET dspaces_fortran PROPERTY OUTPUT_NAME dspaces_f)

target_include_directories(dspaces_fortran
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_Fortran_MODULE_DIRECTORY}>        
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(dspaces_fortran
    PRIVATE
        dspaces
)

install (TARGETS dspaces_fortran EXPORT dspaces-targets
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib)

install (
    DIRECTORY ${CMAKE_Fortran_MODULE_DIRECTORY}/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING
        PATTERN "dspaces*.mod"
        PATTERN "CMakeFiles" EXCLUDE
)

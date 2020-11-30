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
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(dspaces_fortran
    PRIVATE
        dspaces
)

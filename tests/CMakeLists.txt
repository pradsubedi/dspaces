add_executable(dspaces_server server.c)
target_link_libraries(dspaces_server dspaces dspaces-server)

add_executable(test_reader test_reader.c test_get_run.c timer.c)
target_link_libraries(test_reader dspaces)

add_executable(test_writer test_writer.c test_put_run.c timer.c)
target_link_libraries(test_writer dspaces)

add_executable(terminator terminator.c)
target_link_libraries(terminator dspaces)

add_executable(test_writer_f test_writer.f90)
target_link_libraries(test_writer_f dspaces_fortran)

add_executable(test_reader_f test_reader.f90)
target_link_libraries(test_reader_f dspaces_fortran)

add_executable(test_sub test_sub.c test_sub_run.c timer.c)
target_link_libraries(test_sub dspaces)

add_executable(test_writer_server test_writer_server.c test_put_run.c timer.c)
target_link_libraries(test_writer_server dspaces dspaces-server)

add_executable(test_writer_dyn test_writer_dyn.c test_put_dyn_run.c timer.c)
target_link_libraries(test_writer_dyn dspaces)

install(TARGETS dspaces_server terminator
  DESTINATION ${CMAKE_INSTALL_BINDIR}
)

configure_file(
  ${PROJECT_SOURCE_DIR}/tests/test_script.sh.in
  ${PROJECT_BINARY_DIR}/tests/test_script.sh
  @ONLY
)

find_program (BASH_PROGRAM bash)

if (BASH_PROGRAM)
  add_test (Test_write ${BASH_PROGRAM} test_script.sh 1)
  set_tests_properties(Test_write PROPERTIES TIMEOUT 30) 
  add_test (Test_write_local ${BASH_PROGRAM} test_script.sh 1 local)
  set_tests_properties(Test_write_local PROPERTIES TIMEOUT 30)
  add_test (Test_read ${BASH_PROGRAM} test_script.sh 2)
  set_tests_properties(Test_read PROPERTIES TIMEOUT 30) 
  add_test (Test_read_data_subset ${BASH_PROGRAM} test_script.sh 3)
  set_tests_properties(Test_read_data_subset PROPERTIES TIMEOUT 30) 
  add_test (Test_read_ts_subset ${BASH_PROGRAM} test_script.sh 4)
  set_tests_properties(Test_read_ts_subset PROPERTIES TIMEOUT 30) 
  add_test (Test_read_early ${BASH_PROGRAM} test_script.sh 5)
  set_tests_properties(Test_read_early PROPERTIES TIMEOUT 30)
  add_test (Test_read_local ${BASH_PROGRAM} test_script.sh 2 local)
  set_tests_properties(Test_read_local PROPERTIES TIMEOUT 30)
  add_test (Test_read_data_subset_local ${BASH_PROGRAM} test_script.sh 3 local)
  set_tests_properties(Test_read_data_subset_local PROPERTIES TIMEOUT 30)
  add_test (Test_read_ts_subset_local ${BASH_PROGRAM} test_script.sh 4 local)
  set_tests_properties(Test_read_ts_subset_local PROPERTIES TIMEOUT 30)
  add_test (Test_read_early_local ${BASH_PROGRAM} test_script.sh 5 local)
  set_tests_properties(Test_read_early_local PROPERTIES TIMEOUT 30)
  add_test (Test_fortran_binding ${BASH_PROGRAM} test_script.sh 6)
  set_tests_properties(Test_fortran_binding PROPERTIES TIMEOUT 30)
  add_test (Test_sub ${BASH_PROGRAM} test_script.sh 7)
  set_tests_properties(Test_sub PROPERTIES TIMEOUT 60)
  add_test (Test_read_alloc ${BASH_PROGRAM} test_script.sh 8)
  set_tests_properties(Test_read_alloc PROPERTIES TIMEOUT 30)
  add_test (Test_read_alloc_local ${BASH_PROGRAM} test_script.sh 8 local)
  set_tests_properties(Test_read_alloc_local PROPERTIES TIMEOUT 30)
  add_test (Test_write_server ${BASH_PROGRAM} test_script.sh 9)
  set_tests_properties(Test_write_server PROPERTIES TIMEOUT 30) 
  add_test (Test_write_server_local ${BASH_PROGRAM} test_script.sh 9 local)
  set_tests_properties(Test_write_server_local PROPERTIES TIMEOUT 30)
  add_test (Test_read_server ${BASH_PROGRAM} test_script.sh 10)
  set_tests_properties(Test_read_server PROPERTIES TIMEOUT 30)
  add_test (Test_read_server_local ${BASH_PROGRAM} test_script.sh 10 local)
  set_tests_properties(Test_read_server_local PROPERTIES TIMEOUT 30)
  add_test (Test_dyn_write ${BASH_PROGRAM} test_script.sh 11)
  set_tests_properties(Test_dyn_write PROPERTIES TIMEOUT 30)
  add_test (Test_dyn_write_2d ${BASH_PROGRAM} test_script.sh 12)
  set_tests_properties(Test_dyn_write_2d PROPERTIES TIMEOUT 30)
endif (BASH_PROGRAM)

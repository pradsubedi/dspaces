add_executable(dspaces_server server.c)
target_link_libraries(dspaces_server dspaces)

add_executable(test_reader test_reader.c test_get_run.c timer.c)
target_link_libraries(test_reader dspaces)

add_executable(test_writer test_writer.c test_put_run.c timer.c)
target_link_libraries(test_writer dspaces)

configure_file(
  ${PROJECT_SOURCE_DIR}/tests/test_script.sh.in
  ${PROJECT_BINARY_DIR}/tests/test_script.sh
  @ONLY
)

find_program (BASH_PROGRAM bash)

if (BASH_PROGRAM)
  add_test (Test_write ${BASH_PROGRAM} test_script.sh 1)
  add_test (Test_read ${BASH_PROGRAM} test_script.sh 2)
  add_test (Test_read_data_subset ${BASH_PROGRAM} test_script.sh 3)
  add_test (Test_read_ts_subset ${BASH_PROGRAM} test_script.sh 4)

  set_property(TEST PROPERTY ENVIRONMENT
"PATH=${CMAKE_RUNTIME_OUTPUT_DIRECTORY}:$ENV{PATH}")
endif (BASH_PROGRAM)

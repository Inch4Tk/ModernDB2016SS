cmake_minimum_required(VERSION 3.1.0 FATAL_ERROR)

set(PROJECT_NAME_STR moderndb)
set(PROJECT_LIB_NAME ${PROJECT_NAME_STR}_lib)
set(PROJECT_TEST_NAME ${PROJECT_NAME_STR}_test)
project(${PROJECT_NAME_STR})

set(CMAKE_INCLUDE_CURRENT_DIR ON)

# Build directory
if(NOT WIN32)
    set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/build)
else()
    set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/buildwin)
endif()
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})

# Make project c++ 11
set(CMAKE_CXX_STANDARD 11)
set(CXX_STANDARD_REQUIRED ON)

#Include GoogleTest external project
configure_file(ext/gtest/CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
add_subdirectory(${CMAKE_BINARY_DIR}/googletest-src
    ${CMAKE_BINARY_DIR}/googletest-build)

enable_testing()
add_subdirectory(src)
add_subdirectory(test)

option(ASSIGN2_VALID "Builds the project with the provided assignment2 validation file." OFF)
if(ASSIGN2_VALID)
    add_subdirectory(ext/assignment2)
endif()

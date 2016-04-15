cmake_minimum_required(VERSION 3.1.0 FATAL_ERROR)

project(ModernDB)

set(CMAKE_INCLUDE_CURRENT_DIR ON)

# Build directory
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/build)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})

# Make project c++ 11
set(CMAKE_CXX_STANDARD 11)
set(CXX_STANDARD_REQUIRED ON)

add_subdirectory(src)
add_subdirectory(test)
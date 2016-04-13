cmake_minimum_required(VERSION 2.8)

project(ModernDB)

set(CMAKE_INCLUDE_CURRENT_DIR ON)

# Build directory
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/build)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR})

add_subdirectory(src)
add_subdirectory(test)
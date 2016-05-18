set(ASS4_TARGET_NAME assignment4)

set(MY_SRCS
    BTreeTest.cpp
    )

include_directories(
    ${PROJECT_SOURCE_DIR}/src
    )

add_executable(${ASS4_TARGET_NAME} ${MY_SRCS})
target_link_libraries(${ASS4_TARGET_NAME} ${PROJECT_LIB_NAME})

# Compiler warnings and pthreads
if ( CMAKE_COMPILER_IS_GNUCC )
    # This is tested
    set_property( TARGET ${ASS4_TARGET_NAME} APPEND_STRING PROPERTY COMPILE_FLAGS -Wall )
	set(THREADS_PREFER_PTHREAD_FLAG ON)
	find_package(Threads REQUIRED)
    target_link_libraries(${ASS4_TARGET_NAME} Threads::Threads)
endif ( CMAKE_COMPILER_IS_GNUCC )
if ( MSVC )
    # This is untested
    set_property( TARGET ${ASS4_TARGET_NAME} APPEND_STRING PROPERTY COMPILE_FLAGS /W3 )
endif ( MSVC )


set(MY_TEST_SRCS
    alltests.cpp
    buffer/buffertest.cpp
	buffer/segmenttest.cpp
    utility/helperstest.cpp
    utility/rwlocktest.cpp
    sql/schematest.cpp
)

include_directories(
    ${PROJECT_SOURCE_DIR}/src
    )

add_executable(${PROJECT_TEST_NAME} ${MY_TEST_SRCS})
add_dependencies(${PROJECT_TEST_NAME} gtest gmock ${PROJECT_LIB_NAME})

# Link to google test libraries
target_link_libraries(${PROJECT_TEST_NAME} gtest gmock)

# Pthread (Unix only) and Compile Warnings
if ( CMAKE_COMPILER_IS_GNUCC )
    # This is tested
    set_property( TARGET ${PROJECT_TEST_NAME} APPEND_STRING PROPERTY COMPILE_FLAGS -Wall )
	set(THREADS_PREFER_PTHREAD_FLAG ON)
	find_package(Threads REQUIRED)
	target_link_libraries(${PROJECT_TEST_NAME} Threads::Threads)
endif ( CMAKE_COMPILER_IS_GNUCC )
if ( MSVC )
    # This is untested
    set_property( TARGET ${PROJECT_TEST_NAME} APPEND_STRING PROPERTY COMPILE_FLAGS /W3 )
endif ( MSVC )

target_link_libraries(${PROJECT_TEST_NAME} ${PROJECT_LIB_NAME})

add_test(NAME db_tests COMMAND ${PROJECT_TEST_NAME})

if (NOT WIN32)
    add_custom_command(TARGET ${PROJECT_TEST_NAME} POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_SOURCE_DIR}/test/sql/testSchema.sql
                       ${CMAKE_BINARY_DIR}/testSchema.sql
                       )
else ()
    add_custom_command(TARGET ${PROJECT_TEST_NAME} POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E copy
                       ${CMAKE_SOURCE_DIR}/test/sql/testSchema.sql
                       ${CMAKE_CURRENT_BINARY_DIR}/testSchema.sql
                       )
endif (NOT WIN32)



# Set all sourcefiles
set(MY_SRCS
	utility/macros.h
	utility/defines.h
	utility/helpers.h
	utility/helpers.cpp
	utility/RWLock.h
	utility/RWLock.cpp
	buffer/BufferManager.h
	buffer/BufferManager.cpp
	buffer/BufferFrame.h
	buffer/BufferFrame.cpp
)

# Add a library containing project source and link to main executable
add_library(${PROJECT_LIB_NAME} ${MY_SRCS})
add_executable(${PROJECT_NAME_STR} main.cpp)
target_link_libraries(${PROJECT_NAME_STR} ${PROJECT_LIB_NAME})

# Compiler warnings and pthreads
if ( CMAKE_COMPILER_IS_GNUCC )
    # This is tested
    set_property( TARGET ${PROJECT_NAME_STR} APPEND_STRING PROPERTY COMPILE_FLAGS -Wall )
	set(THREADS_PREFER_PTHREAD_FLAG ON)
	find_package(Threads REQUIRED)
	target_link_libraries(${PROJECT_NAME_STR} Threads::Threads)
endif ( CMAKE_COMPILER_IS_GNUCC )
if ( MSVC )
    # This is untested
    set_property( TARGET ${PROJECT_NAME_STR} APPEND_STRING PROPERTY COMPILE_FLAGS /W3 )
endif ( MSVC )

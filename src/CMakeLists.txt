
# Set all sourcefiles
set(MY_SRCS
	main.cpp
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

# Add a build executable
add_executable(moderndb ${MY_SRCS})

# Compiler warnings and pthreads
if ( CMAKE_COMPILER_IS_GNUCC )
    # This is tested
    set_property( TARGET moderndb APPEND_STRING PROPERTY COMPILE_FLAGS -Wall )
	set(THREADS_PREFER_PTHREAD_FLAG ON)
	find_package(Threads REQUIRED)
	target_link_libraries(moderndb Threads::Threads)
endif ( CMAKE_COMPILER_IS_GNUCC )
if ( MSVC )
    # This is untested
    set_property( TARGET moderndb APPEND_STRING PROPERTY COMPILE_FLAGS /W3 )
endif ( MSVC )
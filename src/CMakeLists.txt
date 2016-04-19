
# Set all sourcefiles
set(MY_SRCS
	main.cpp
	macros.h
	defines.h
	helpers.h
	helpers.cpp
)

# Add a build executable
add_executable(sort ${MY_SRCS})

# Compiler warnings
if ( CMAKE_COMPILER_IS_GNUCC )
    # This is tested
    set_property( TARGET sort APPEND_STRING PROPERTY COMPILE_FLAGS -Wall )
endif ( CMAKE_COMPILER_IS_GNUCC )
if ( MSVC )
    # This is untested
    set_property( TARGET sort APPEND_STRING PROPERTY COMPILE_FLAGS /W3 )
endif ( MSVC )
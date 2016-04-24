#include "utility/helpers.h"

#include "gtest/gtest.h"

#include <fstream>

TEST( FileExistsTest, NotExisting )
{
	bool res = FileExists( "asdfasdfdgtwasdfasdfsadfqwerqwer" );
	EXPECT_EQ( false, res );
}

TEST( FileExistsTest, Existing )
{
	std::string filename = "asdfasdfzdgjuasdfqwerqwer";
	// Create a file
	std::ofstream newfile;
	newfile.open( filename, std::ofstream::out | std::ofstream::binary );
	newfile.close();

	bool res = FileExists( filename );
	EXPECT_EQ( true, res );
	std::remove( filename.c_str() );
}
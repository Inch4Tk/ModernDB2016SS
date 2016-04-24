#include "utility/helpers.h"

#include "gtest/gtest.h"

#include <fstream>

TEST( FileExistsTest, NotExisting )
{
	bool res = FileExists( std::string( "asdfasdfdgtwasdfasdfsadfqwerqwer" ) );
	EXPECT_FALSE( res );
}

TEST( FileExistsTest, Existing )
{
	std::string filename = "asdfasdfzdgjuasdfqwerqwer";
	// Create a file
	std::ofstream newfile;
	newfile.open( filename, std::ofstream::out | std::ofstream::binary );
	newfile.close();

	bool res = FileExists( filename );
	EXPECT_TRUE( res );
	std::remove( filename.c_str() );
}
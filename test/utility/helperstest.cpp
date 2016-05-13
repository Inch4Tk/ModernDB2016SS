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

TEST( TupleId, MergeSplitTID )
{
	// Except both ways to result in the same things
	uint64_t id = MergeTID( 3, 1256 );
	auto split = SplitTID( id );
	EXPECT_EQ( 3, split.first );
	EXPECT_EQ( 1256, split.second );

	split = SplitTID( 0x000500000000005Ful );
	id = MergeTID( split.first, split.second );
	EXPECT_EQ( 0x000500000000005Ful, id );

	// Expect merge to cut away things that are too big
	id = MergeTID( 0x000FFFFFFFFFFFFFul, 0xFFFFFul );
	split = SplitTID( id );
	EXPECT_EQ( 0x0000FFFFFFFFFFFFul, split.first );
	EXPECT_EQ( 0xFFFFul, split.second );
}
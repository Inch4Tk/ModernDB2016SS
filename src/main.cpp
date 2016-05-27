
#include "utility/defines.h"
#include "utility/helpers.h"
#include "DBCore.h"
#include "sql/Schema.h"
#include <iostream>
#include <string>
#include <stdint.h>
#include <assert.h>

uint32_t binarySearch(uint64_t key, std::vector<uint64_t>& nums)
{
	uint32_t start = 0;
	uint32_t end = nums.size();
	while ( true )
	{
		if ( start == end )
		{
			return start;
		}
		uint32_t current = (end - start) / 2 + start; // Prevent overflows

		uint64_t currentKey = nums[current];

		if ( key < currentKey )
		{
			end = current;
		}
		else if ( key > currentKey )
		{
			start = current + 1;
		}
		else
		{
			return current;
		}
	}
}


int main( int argc, char* argv[] )
{
	// Windows memory leak detection debug system call
#ifdef _CRTDBG_MAP_ALLOC
	_CrtSetDbgFlag( _CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF );
#endif // _CRTDBG_MAP_ALLOC

	DBCore core;

	// Testing binary search
	std::vector<uint64_t> nums( {2, 5, 6, 11, 14, 17, 19, 21} );
	uint32_t idx0 = binarySearch( 0, nums );
	assert( idx0 == 0 );
	uint32_t idx1 = binarySearch( 4, nums );
	assert( idx1 == 1 );
	uint32_t idx2 = binarySearch( 5, nums );
	assert( idx2 == 1 );
	uint32_t idx3 = binarySearch( 14, nums );
	assert( idx3 == 4 );
	uint32_t idx4 = binarySearch( 20, nums );
	assert( idx4 == 7 );
	uint32_t idx5 = binarySearch( 21, nums );
	assert( idx5 == 7 );
	uint32_t idx6 = binarySearch( 22, nums );
	assert( idx6 == 8 );
	uint32_t idx7 = binarySearch( 105, nums );
	assert( idx7 == 8 );
	nums.push_back( 25 );
	uint32_t idx8 = binarySearch( 0, nums );
	assert( idx8 == 0 );
	uint32_t idx9 = binarySearch( 4, nums );
	assert( idx9 == 1 );
	uint32_t idx10 = binarySearch( 5, nums );
	assert( idx10 == 1 );
	uint32_t idx11 = binarySearch( 14, nums );
	assert( idx11 == 4 );
	uint32_t idx12 = binarySearch( 20, nums );
	assert( idx12 == 7 );
	uint32_t idx13 = binarySearch( 21, nums );
	assert( idx13 == 7 );
	uint32_t idx14 = binarySearch( 22, nums );
	assert( idx14 == 8 );
	uint32_t idx15 = binarySearch( 105, nums );
	assert( idx15 == 9 );

	std::vector<uint64_t> otherNums;
	uint32_t idx16 = binarySearch( 0, otherNums );
	assert( idx16 == 0 );
	uint32_t idx17 = binarySearch( 123, otherNums );
	assert( idx17 == 0 );
	otherNums.push_back( 0 );
	uint32_t idx18 = binarySearch( 0, otherNums );
	assert( idx18 == 0 );
	uint32_t idx19 = binarySearch( 123, otherNums );
	assert( idx19 == 1 );
	otherNums.clear();
	otherNums.push_back( 23 );
	uint32_t idx20 = binarySearch( 0, otherNums );
	assert( idx20 == 0 );
	uint32_t idx21 = binarySearch( 23, otherNums );
	assert( idx21 == 0 );
	uint32_t idx22 = binarySearch( 123, otherNums );
	assert( idx22 == 1 );

	std::vector<uint64_t> moreNums( { 0,1,2,3 } );
	uint32_t idx23 = binarySearch( 0, moreNums );
	assert( idx23 == 0 );
	uint32_t idx24 = binarySearch( 2, moreNums );
	assert( idx24 == 2 );
	uint32_t idx25 = binarySearch( 3, moreNums );
	assert( idx25 == 3 );
	uint32_t idx26 = binarySearch( 4, moreNums );
	assert( idx26 == 4 );
	uint32_t idx27 = binarySearch( 5, moreNums );
	assert( idx27 == 4 );
	moreNums.push_back( 4 );
	uint32_t idx28 = binarySearch( 0, moreNums );
	assert( idx28 == 0 );
	uint32_t idx29 = binarySearch( 2, moreNums );
	assert( idx29 == 2 );
	uint32_t idx30 = binarySearch( 3, moreNums );
	assert( idx30 == 3 );
	uint32_t idx31 = binarySearch( 4, moreNums );
	assert( idx31 == 4 );
	uint32_t idx32 = binarySearch( 5, moreNums );
	assert( idx32 == 5 );

	return 0;
}
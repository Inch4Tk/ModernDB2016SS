#include <string>
#include <cstdint>
#include <cassert>
#include <vector>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <utility>

#include "utility/helpers.h"
#include "utility/defines.h"
#include "index/BPTree.h"
#include "DBCore.h"

/* Comparator functor for uint64_t*/
struct MyCustomUInt64Cmp
{
	bool operator()( uint64_t a, uint64_t b ) const
	{
		return a < b;
	}
};

template <unsigned len>
struct MyChar
{
	char data[len];
};

/* Comparator functor for char */
template <unsigned len>
struct MyCustomCharCmp
{
	bool operator()( const MyChar<len>& a, const MyChar<len>& b ) const
	{
		return memcmp( a.data, b.data, len ) < 0;
	}
};

typedef std::pair<uint32_t, uint32_t> IntPair;

/* Comparator for IntPair */
struct MyCustomIntPairCmp
{
	bool operator()( const IntPair& a, const IntPair& b ) const
	{
		if ( a.first < b.first )
			return true;
		else
			return (a.first == b.first) && (a.second < b.second);
	}
};

template <class T>
const T& getKey( const uint64_t& i );

template <>
const uint64_t& getKey( const uint64_t& i )
{
	return i;
}

std::vector<std::string> char20;
template <>
const MyChar<20>& getKey( const uint64_t& i )
{
	std::stringstream ss;
	ss << i;
	std::string s( ss.str() );
	char20.push_back( std::string( 20 - s.size(), '0' ) + s );
	return *reinterpret_cast<const MyChar<20>*>(char20.back().data());
}

std::vector<IntPair> intPairs;
template <>
const IntPair& getKey( const uint64_t& i )
{
	intPairs.push_back( std::make_pair( static_cast<uint32_t>(i / 3), static_cast<uint32_t>(3 - (i % 3)) ) );
	return intPairs.back();
}


template <class T, class CMP>
void test( uint64_t n )
{
	DBCore core;
	core.WipeDatabase();
	// Wipe old sql schema and data to get consisten records
	// Then create new sql schema, just containing large strings, currently
	// this is not perfectly tied together anyways, so insert does not 
	// care that our records are not correct, but we still need a relation
	// before we can get a segment for that relation
	// (Mb we should make segments that are not bound to a relation at some
	// point)
	std::string sql = "create table dbtest ( strentry char(5000), primary key (strentry));";
	core.AddRelationsFromString( sql );
	uint64_t segmentId = core.GetSegmentOfIndex( "dbtest", "strentry" );
	BPTree<T, CMP> bTree( core, *core.GetBufferManager(), segmentId );

	// Insert values
	for ( uint64_t i = 0; i < n; ++i )
		bTree.Insert( getKey<T>( i ), static_cast<TID>(i*i) );
	assert( bTree.GetSize() == n );

	// Check if they can be retrieved
	for ( uint64_t i = 0; i < n; ++i )
	{
		std::pair<bool, TID> foundTID;
		foundTID = bTree.Lookup( getKey<T>( i ) );
		assert( foundTID.first );
		assert( foundTID.second == i*i );
	}

	// Delete some values
	for ( uint64_t i = 0; i < n; ++i )
		if ( (i % 7) == 0 )
			bTree.Erase( getKey<T>( i ) );

	// Check if the right ones have been deleted
	for ( uint64_t i = 0; i < n; ++i )
	{
		std::pair<bool, TID> foundTID;
		foundTID = bTree.Lookup( getKey<T>( i ) );
		if ( (i % 7) == 0 )
		{
			assert( !foundTID.first );
		}
		else
		{
			assert( foundTID.first );
			assert( foundTID.second == i*i );
		}
	}

	// Delete everything
	for ( uint64_t i = 0; i < n; ++i )
		bTree.Erase( getKey<T>( i ) );
	assert( bTree.GetSize() == 0 );
}

int main( int argc, char* argv[] )
{
	// Get command line argument
	const uint64_t n = (argc == 2) ? strtoul( argv[1], NULL, 10 ) : 1000 * 1000ul;

	// Test index with 64bit unsigned integers
	test<uint64_t, MyCustomUInt64Cmp>( n );

	// Test index with 20 character strings
	test<MyChar<20>, MyCustomCharCmp<20>>( n );

	// Test index with compound key
	test<IntPair, MyCustomIntPairCmp>( n );
	return 0;
}

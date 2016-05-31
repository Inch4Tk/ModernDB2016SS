#include "index/BPTree.h"
#include "utility/macros.h"
#include "utility/defines.h"

#include "gtest/gtest.h"

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


// Test that the b+ tree works
template <class Types>
class BPTreeTest : public ::testing::Test
{
public:
	virtual void SetUp() override
	{
		core = new DBCore();
		core->WipeDatabase();

		std::string sql = "create table dbtest ( strentry char(5000), primary key (strentry));";
		core->AddRelationsFromString( sql );
		uint64_t segmentId = core->GetSegmentOfIndex( "dbtest", "strentry" );
		bTree = new BPTree<typename Types::T, typename Types::CMP>( *core, *(core->GetBufferManager()), segmentId );
	}
	virtual void TearDown() override
	{
		SDELETE( core );
	}
	DBCore* core;
	BPTree<typename Types::T, typename Types::CMP>* bTree;
};

template <typename A, typename B>
struct TypeDefinitions
{
	typedef typename A T;
	typedef typename B CMP;
};

typedef ::testing::Types<TypeDefinitions<uint64_t, MyCustomUInt64Cmp>,
	TypeDefinitions<MyChar<20>, MyCustomCharCmp<20>>,
	TypeDefinitions<IntPair, MyCustomIntPairCmp>> MyTypes;
TYPED_TEST_CASE( BPTreeTest, MyTypes );

TYPED_TEST( BPTreeTest, BPTreeFull )
{
	uint64_t n = 100000;

	// Insert values
	for ( uint64_t i = 0; i < n; ++i )
	{
		bTree->Insert( getKey<typename TypeParam::T>( i ), static_cast<TID>(i*i) );
	}
	uint32_t btreeSize = bTree->GetSize();
	EXPECT_EQ( btreeSize, n );

	// Check if they can be retrieved
	for ( uint64_t i = 0; i < n; ++i )
	{
		std::pair<bool, TID> foundTID;
		foundTID = bTree->Lookup( getKey<typename TypeParam::T>( i ) );
		EXPECT_TRUE( foundTID.first );
		EXPECT_EQ( foundTID.second, i*i );
	}

	// Delete some values
	for ( uint64_t i = 0; i < n; ++i )
		if ( (i % 7) == 0 )
			bTree->Erase( getKey<typename TypeParam::T>( i ) );

	// Check if the right ones have been deleted
	for ( uint64_t i = 0; i < n; ++i )
	{
		std::pair<bool, TID> foundTID;
		foundTID = bTree->Lookup( getKey<typename TypeParam::T>( i ) );
		if ( (i % 7) == 0 )
		{
			EXPECT_TRUE( !foundTID.first );
		}
		else
		{
			EXPECT_TRUE( foundTID.first );
			EXPECT_EQ( foundTID.second, i*i );
		}
	}

	// Delete everything
	for ( uint64_t i = 0; i < n; ++i )
		bTree->Erase( getKey<typename TypeParam::T>( i ) );
	EXPECT_EQ( bTree->GetSize(), 0 );
}
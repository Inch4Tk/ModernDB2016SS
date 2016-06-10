#include "DBCore.h"
#include "buffer/SPSegment.h"
#include "utility/macros.h"
#include "utility/helpers.h"

#include "query/Register.h"
#include "query/QueryOperator.h"
#include "query/TableScanOperator.h"
#include "query/PrintOperator.h"

#include "gtest/gtest.h"

#include <unordered_map>
#include "query/ProjectionOperator.h"
#include "query/SelectOperator.h"
#include "query/HashJoinOperator.h"

std::vector<std::string> firstNames = {
	"Jack",
	"Donald",
	"Dieter",
	"Günther",
	"Peter",
	"Detlef",
	"Christiano",
	"Olaf",
	"Mandy",
	"Jacqueline",
	"Leif",
	"Dante",
	"Alegra",
	"Mercedes",
	"Khadija",
	"Arnold",
	"Hillary",
	"Angela",
	"Francis"
};
std::vector<std::string> lastNames = {
	"Trump",
	"Sparrow",
	"Pan",
	"Armstrong",
	"Schwarzenegger",
	"McCain",
	"Clinton",
	"Merkel",
	"Underwood",
	"Ronaldo",
	"Alighieri",
	"Terminator",
	"ASDF",
	"Smith",
	"Obama",
	"Gustavsson",
	"AnotherRandomLastName"
};
std::vector<std::string> randomStrings = {
	"hBOggQ2EAzOuRN",
	"Y9VJzxVc23B8WE",
	"Piaf3Sy9x1zmK3",
	"czskvaBwggVOVo",
	"YW6MWL1Ru9spoY",
	"pODmPNPJ6b8i8a",
	"um9yrQfrCEKNe6",
	"wWQSFS7qqgYBLG",
	"hpNHfc3Y3sRVjF",
	"oDaAsSKbChsuHy"
};

// Test our queries
class QueryTest : public ::testing::Test
{
public:
	virtual void SetUp() override
	{
		srand( static_cast<uint32_t>(time( NULL )) );
		core = new DBCore();
		core->WipeDatabase();
		// Create the following tables (actually the char lengths are not strictly necessary, because they are pretty much ignored anyways as far as I can remember)
		//////
		// dbtestA
		// name: char(50), primary key
		// age: integer
		// somefield: char(30)
		// lastfield: integer
		//////
		// dbtestB
		// bestfield: integer, primary key
		// age: integer
		// bcharfield: char(30)
		//////
		std::string sql = std::string( "create table dbtestA ( name char(50), age integer, somefield char(30), lastfield integer, primary key (lastfield) );\n" ) +
			"create table dbtestB ( bestfield integer, age integer, bcharfield char(30), primary key (bestfield) );\n" +
			"create table dbtestOrderPreserv ( name char(50), age integer, somefield char(30), lastfield integer, primary key (lastfield) );";
		core->AddRelationsFromString( sql );

		// Add a few hundred random entries to both relations so we can do some multipage queries
		std::unique_ptr<SPSegment> spA = core->GetSPSegment( "dbtestA" );
		for ( uint32_t i = 0; i < 750; ++i)
		{
			spA->Insert( GenerateRecordA() );
		}
		std::unique_ptr<SPSegment> spB = core->GetSPSegment( "dbtestB" );
		for ( uint32_t i = 0; i < 750; ++i )
		{
			spB->Insert( GenerateRecordB() );
		}
		// Special relation that is the same as dbtestA, but used in tests where we need to preserve the order of inserts for easier testing
		// (inserts can swap order if strings have different sizes, so one tuple still fits a hole that another doesn't)
		std::unique_ptr<SPSegment> spOrderpreserv = core->GetSPSegment( "dbtestOrderPreserv" );
		for ( uint32_t i = 0; i < 750; ++i )
		{
			spOrderpreserv->Insert( GenerateRecordOrder() );
		}
	}
	virtual void TearDown() override
	{
		SDELETE( core );
	}
	DBCore* core;
	std::vector<std::string> nameA;
	std::vector<Integer> ageA;
	std::vector<std::string> somefieldA;
	std::vector<Integer> lastfieldA;

	std::vector<Integer> bestfieldB;
	std::vector<Integer> ageB;
	std::vector<std::string> bcharfieldB;

	std::vector<std::string> nameOrder;
	std::vector<Integer> ageOrder;
	std::vector<std::string> somefieldOrder;
	std::vector<Integer> lastfieldOrder;

	Record GenerateRecordA()
	{
		static uint32_t ctr = 0;
		std::vector<uint8_t> data;
		// name
		uint32_t n1 = rand() % firstNames.size();
		uint32_t n2 = rand() % lastNames.size();
		nameA.push_back( firstNames[n1] + " " + lastNames[n2] );
		AppendToData( firstNames[n1] + " " + lastNames[n2], data );

		// age
		Integer r = rand() % 100;
		ageA.push_back( r );
		AppendToData( r, data );

		// somefield
		std::string chosen = randomStrings[rand() % randomStrings.size()];
		somefieldA.push_back( chosen );
		AppendToData( chosen, data );

		// lastfield
		r = ctr;
		lastfieldA.push_back( r );
		AppendToData( r, data );

		++ctr;
		return Record( static_cast<uint32_t>(data.size()), &data[0] );
	}
	Record GenerateRecordB()
	{
		std::vector<uint8_t> data;
		// bestfield
		Integer r = rand();
		bestfieldB.push_back( r );
		AppendToData( r, data );

		// age
		r = rand() % 100;
		ageB.push_back( r );
		AppendToData( r, data );

		// bcharfield
		std::string chosen = randomStrings[rand() % randomStrings.size()];
		bcharfieldB.push_back( chosen );
		AppendToData( chosen, data );

		return Record( static_cast<uint32_t>(data.size()), &data[0] );
	}
	Record GenerateRecordOrder()
	{
		std::vector<uint8_t> data;
		// name
		uint32_t n1 = rand() % firstNames.size();
		uint32_t n2 = rand() % lastNames.size();
		std::string ins = firstNames[n1] + " " + lastNames[n2];
		PadRightTo( ins, 50 );
		nameOrder.push_back( ins );
		AppendToData( ins, data );

		// age
		Integer r = rand() % 90;
		ageOrder.push_back( r );
		AppendToData( r, data );

		// somefield
		std::string chosen = randomStrings[rand() % randomStrings.size()];
		PadRightTo( chosen, 30 );
		somefieldOrder.push_back( chosen );
		AppendToData( chosen, data );

		// lastfield
		r = rand();
		lastfieldOrder.push_back( r );
		AppendToData( r, data );

		return Record( static_cast<uint32_t>(data.size()), &data[0] );
	}

	void AppendToData( uint32_t toappend, std::vector<uint8_t>& data )
	{
		uint8_t* ptr = reinterpret_cast<uint8_t*>(&toappend);
		data.push_back( ptr[0] );
		data.push_back( ptr[1] );
		data.push_back( ptr[2] );
		data.push_back( ptr[3] );
	}

	void AppendToData( const std::string& toappend, std::vector<uint8_t>& data )
	{
		AppendToData( static_cast<uint32_t>(toappend.size()), data );
		const char* ptr = toappend.c_str();
		for ( uint32_t i = 0; i < toappend.size(); ++i )
		{
			data.push_back( ptr[i] );
		}
	}

	void PadRightTo( std::string &str, const size_t num, const char paddingChar = ' ' )
	{
		if ( num > str.size() )
			str.insert( str.size(), num - str.size(), paddingChar );
	}
};

// There is a single test for every query directly after a table scan to test principal correctness
// In the end there is one big query chaining all the queries together

// Test if the table scan works correctly
// This also tests correct setup of registers
TEST_F( QueryTest, TableScanQuery )
{
	TableScanOperator op( "dbtestOrderPreserv", *core, *core->GetBufferManager() );
	op.Open();
	std::vector<Register*> registers = op.GetOutput();
	EXPECT_EQ( registers.size(), 4 );
	// Names
	EXPECT_EQ( registers[0]->GetAttributeName(), std::string( "name" ) );
	EXPECT_EQ( registers[1]->GetAttributeName(), std::string( "age" ) );
	EXPECT_EQ( registers[2]->GetAttributeName(), std::string( "somefield" ) );
	EXPECT_EQ( registers[3]->GetAttributeName(), std::string( "lastfield" ) );
	// Types
	EXPECT_EQ( registers[0]->GetType(), SchemaTypes::Tag::Char );
	EXPECT_EQ( registers[1]->GetType(), SchemaTypes::Tag::Integer );
	EXPECT_EQ( registers[2]->GetType(), SchemaTypes::Tag::Char );
	EXPECT_EQ( registers[3]->GetType(), SchemaTypes::Tag::Integer );

	uint32_t curidx = 0;
	while (op.Next())
	{
		EXPECT_EQ( registers[0]->GetString(), nameOrder[curidx] );
		EXPECT_EQ( registers[1]->GetInteger(), ageOrder[curidx] );
		EXPECT_EQ( registers[2]->GetString(), somefieldOrder[curidx] );
		EXPECT_EQ( registers[3]->GetInteger(), lastfieldOrder[curidx] );
		++curidx;
	}
	EXPECT_EQ( curidx, 750 );
	op.Close();
}

// This tests that the table scan still works after deletes and updates
TEST_F( QueryTest, TableScanAfterDeleteUpdates )
{
	// We make a test that every still existing value will be returned but no more and order does not matter
	// Use a different database to make things much more simple
	core->WipeDatabase();
	std::vector<TID> tids;
	std::unordered_map<TID, uint32_t> tidmapping;
	std::unordered_map<uint32_t, bool> shouldexist;
	std::unordered_map<uint32_t, bool> deleted;
	std::string sql = std::string( "create table dbtestA ( int integer, primary key (int) );" );
	core->AddRelationsFromString( sql );

	std::unique_ptr<SPSegment> spA = core->GetSPSegment( "dbtestA" );
	for ( uint32_t i = 0; i < 3000; ++i )
	{
		TID t = spA->Insert( Record( 4, reinterpret_cast<uint8_t*>(&i) ) );
		tids.push_back( t );
		tidmapping.insert( std::make_pair( t, i ) );
		shouldexist.insert( std::make_pair( i, true ) );
	}

	for ( uint32_t i = 0; i < 1500; ++i )
	{
		uint32_t targetIdx = rand() % tids.size();
		TID target = tids[targetIdx];
		if (rand() % 2 == 1)
		{
			// Delete
			tids[targetIdx] = tids[tids.size() - 1];
			tids.pop_back();
			shouldexist[tidmapping[target]] = false;
			//deleted.insert( std::make_pair( tidmapping[target], true ) );
			tidmapping.erase(target);
			spA->Remove( target );
		}
		else
		{
			// Update
			uint32_t old = tidmapping[target];
			uint32_t newval = i + 3000;
			tidmapping[target] = newval;
			shouldexist[old] = false;
			shouldexist.insert( std::make_pair( newval, true ) );
			spA->Update( target, Record( 4, reinterpret_cast<uint8_t*>(&newval) ) );
		}
	}

	// Make sure our bookkeeping is correct
	// We expect exactly InsertCount - DeleteCount truthy values
	// DeleteCount = 4500 - Shouldexist.size, because only delete does not add anything to shouldexist
	uint32_t existing = 0;
	for ( auto pair : shouldexist )
	{
		if (pair.second)
		{
			existing++;
		}
	}
	EXPECT_EQ( existing, 3000 - (4500 - shouldexist.size()) );

	TableScanOperator op( "dbtestA", *core, *core->GetBufferManager() );
	op.Open();
	std::vector<Register*> registers = op.GetOutput();
	uint32_t curidx = 0;
	while ( op.Next() )
	{
		Integer i = registers[0]->GetInteger();
		auto it = shouldexist.find( i );
		if (it != shouldexist.end())
		{
			EXPECT_EQ( it->second, true ); // If we found it we expect it should be there
			shouldexist[i] = false; // We only want to see everything once
		}
		else
		{
			EXPECT_TRUE( false ); // should always find every entry
		}
		
		++curidx;
	}
	// Make a pass over our map and check that we found everything (everything is set to false)
	for (auto pair : shouldexist)
	{
		EXPECT_FALSE( pair.second );
	}

	op.Close();
}

// Test print operator
TEST_F(QueryTest, PrintOperatorQuery)
{
	// Extract the string to every tuple and test it against the order preserved db
	TableScanOperator tsop( "dbtestOrderPreserv", *core, *core->GetBufferManager() );
	std::stringstream ss;
	PrintOperator prop( tsop, ss );
	prop.Open();
	std::vector<Register*> registers = prop.GetOutput();
	uint32_t curidx = 0;
	while ( prop.Next() )
	{
		std::string got( std::istreambuf_iterator<char>( ss ), {} );
		std::string expected = nameOrder[curidx] + ", " + 
			std::to_string(ageOrder[curidx]) + ", " +
			somefieldOrder[curidx] + ", " + std::to_string( lastfieldOrder[curidx]) + "\n";
		EXPECT_EQ( expected, got );
		++curidx;
	}
	prop.Close();
}

// Test projection operator
TEST_F( QueryTest, ProjectionOperatorQuery )
{
	// Project to fewer and change order
	TableScanOperator tsop0( "dbtestOrderPreserv", *core, *core->GetBufferManager() );
	ProjectionOperator prop0( tsop0, std::vector<std::string>( { "somefield", "name" } ) );
	prop0.Open();
	std::vector<Register*> registers0 = prop0.GetOutput();
	EXPECT_EQ( registers0[0]->GetType(), SchemaTypes::Tag::Char );
	EXPECT_EQ( registers0[1]->GetType(), SchemaTypes::Tag::Char );
	EXPECT_EQ( registers0[0]->GetAttributeName(), std::string( "somefield" ) );
	EXPECT_EQ( registers0[1]->GetAttributeName(), std::string( "name" ) );
	// Fetch first value and check if that is correct
	prop0.Next();
	EXPECT_EQ( registers0[0]->GetString(), somefieldOrder[0] );
	EXPECT_EQ( registers0[1]->GetString(), nameOrder[0] );
	prop0.Close();

	// Project a field multiple times
	TableScanOperator tsop1( "dbtestOrderPreserv", *core, *core->GetBufferManager() );
	ProjectionOperator prop1( tsop1, std::vector<std::string>( { "name", "somefield", "age", "age", "name" } ) );
	prop1.Open();
	std::vector<Register*> registers1 = prop1.GetOutput();
	EXPECT_EQ( registers1[0]->GetType(), SchemaTypes::Tag::Char );
	EXPECT_EQ( registers1[1]->GetType(), SchemaTypes::Tag::Char );
	EXPECT_EQ( registers1[2]->GetType(), SchemaTypes::Tag::Integer );
	EXPECT_EQ( registers1[3]->GetType(), SchemaTypes::Tag::Integer );
	EXPECT_EQ( registers1[4]->GetType(), SchemaTypes::Tag::Char );
	EXPECT_EQ( registers1[0]->GetAttributeName(), std::string( "name" ) );
	EXPECT_EQ( registers1[1]->GetAttributeName(), std::string( "somefield" ) );
	EXPECT_EQ( registers1[2]->GetAttributeName(), std::string( "age" ) );
	EXPECT_EQ( registers1[3]->GetAttributeName(), std::string( "age" ) );
	EXPECT_EQ( registers1[4]->GetAttributeName(), std::string( "name" ) );
	// Fetch first value and check if that is correct
	prop1.Next();
	EXPECT_EQ( registers1[0]->GetString(), nameOrder[0] );
	EXPECT_EQ( registers1[1]->GetString(), somefieldOrder[0] );
	EXPECT_EQ( registers1[2]->GetInteger(), ageOrder[0] );
	EXPECT_EQ( registers1[3]->GetInteger(), ageOrder[0] );
	EXPECT_EQ( registers1[4]->GetString(), nameOrder[0] );
	prop1.Close();
}

// Test selection operator
TEST_F( QueryTest, SelectOperatorQuery )
{
	std::vector<uint32_t> indices;
	// Manually find every entry in order preserve relation that is of age 30
	for ( uint32_t i = 0; i < ageOrder.size(); ++i )
	{
		if (ageOrder[i] == 30)
		{
			indices.push_back( i );
		}
	}

	TableScanOperator tsop( "dbtestOrderPreserv", *core, *core->GetBufferManager() );
	SelectOperator sop( tsop, "age", 30u );
	sop.Open();
	std::vector<Register*> registers = sop.GetOutput();
	uint32_t curidx = 0;
	while (sop.Next())
	{
		// Compare fields to check if it is the same entry
		EXPECT_EQ( registers[0]->GetString(), nameOrder[indices[curidx]] );
		EXPECT_EQ( registers[1]->GetInteger(), ageOrder[indices[curidx]] );
		EXPECT_EQ( registers[2]->GetString(), somefieldOrder[indices[curidx]] );
		EXPECT_EQ( registers[3]->GetInteger(), lastfieldOrder[indices[curidx]] );
		++curidx;
	}
	sop.Close();
}

// Test Hash join Operator
TEST_F( QueryTest, HashJoinOperatorQuery )
{
	// Build the results
	std::unordered_multimap<Integer, uint32_t> ageToIndex;
	std::unordered_multimap<Integer, std::pair<uint32_t, uint32_t>> results; // Map age to two indices

	for ( uint32_t i = 0; i < ageA.size(); ++i )
	{
		ageToIndex.insert( std::make_pair( ageA[i], i ) );
	}
	for ( uint32_t i = 0; i < ageB.size(); ++i )
	{
		auto it = ageToIndex.equal_range( ageB[i] );
		while (it.first != it.second)
		{
			// Produce pairs
			results.insert( std::make_pair( it.first->first, std::make_pair( it.first->second, i ) ) );
			++(it.first);
		}
	}

	// Do the database ops
	TableScanOperator tsopA( "dbtestA", *core, *core->GetBufferManager() );
	TableScanOperator tsopB( "dbtestB", *core, *core->GetBufferManager() );
	HashJoinOperator hjop( tsopA, tsopB, "age", "age" ); // Join all same age pairs
	hjop.Open();
	std::vector<Register*> registers = hjop.GetOutput();
	// Check registers
	EXPECT_EQ( registers.size(), 6 );
	// Names
	EXPECT_EQ( registers[0]->GetAttributeName(), std::string( "name" ) );
	EXPECT_EQ( registers[1]->GetAttributeName(), std::string( "age" ) );
	EXPECT_EQ( registers[2]->GetAttributeName(), std::string( "somefield" ) );
	EXPECT_EQ( registers[3]->GetAttributeName(), std::string( "lastfield" ) );
	EXPECT_EQ( registers[4]->GetAttributeName(), std::string( "bestfield" ) );
	EXPECT_EQ( registers[5]->GetAttributeName(), std::string( "bcharfield" ) );
	// Types
	EXPECT_EQ( registers[0]->GetType(), SchemaTypes::Tag::Char );
	EXPECT_EQ( registers[1]->GetType(), SchemaTypes::Tag::Integer );
	EXPECT_EQ( registers[2]->GetType(), SchemaTypes::Tag::Char );
	EXPECT_EQ( registers[3]->GetType(), SchemaTypes::Tag::Integer );
	EXPECT_EQ( registers[4]->GetType(), SchemaTypes::Tag::Integer );
	EXPECT_EQ( registers[5]->GetType(), SchemaTypes::Tag::Char );
	while ( hjop.Next() )
	{
		// Compare the result to all potential result candidates. We have to find the candidate. 
		// If we found the candidate, delete it from results set
		auto it = results.equal_range( registers[1]->GetInteger() );
		bool found = false;
		while ( it.first != it.second )
		{
			uint32_t idxA = it.first->second.first;
			uint32_t idxB = it.first->second.second;
			if ( registers[0]->GetString() == nameA[idxA] &&
				 registers[1]->GetInteger() == ageA[idxA] && 
				 registers[1]->GetInteger() == ageB[idxB] &&
				 registers[2]->GetString() == somefieldA[idxA] && 
				 registers[3]->GetInteger() == lastfieldA[idxA] && 
				 registers[4]->GetInteger() == bestfieldB[idxB] &&
				 registers[5]->GetString() == bcharfieldB[idxB] )
			{
				results.erase( it.first );
				found = true;
				break;
			}
			++(it.first);
		}
		EXPECT_TRUE( found );
	}
	EXPECT_EQ( 0, results.size() );
	hjop.Close();
}
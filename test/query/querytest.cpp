#include "DBCore.h"
#include "buffer/SPSegment.h"
#include "utility/macros.h"
#include "utility/helpers.h"

#include "query/Register.h"
#include "query/TableScanOperator.h"

#include "gtest/gtest.h"

#include <unordered_map>

std::vector<std::string> firstNames = {
	"Jack",
	"Donald",
	"Dieter",
	"G�nther",
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
		// Special relation that is the same as dbtestA, but used in tests where we need to preserve the order of inserts
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
		std::vector<uint8_t> data;
		// name
		uint32_t n1 = rand() % firstNames.size();
		uint32_t n2 = rand() % lastNames.size();
		nameA.push_back( firstNames[n1] + " " + lastNames[n2] );
		AppendToData( firstNames[n1] + " " + lastNames[n2], data );

		// age
		Integer r = rand() % 90;
		ageA.push_back( r );
		AppendToData( r, data );

		// somefield
		std::string chosen = randomStrings[rand() % randomStrings.size()];
		somefieldA.push_back( chosen );
		AppendToData( chosen, data );

		// lastfield
		r = rand();
		lastfieldA.push_back( r );
		AppendToData( r, data );

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
		r = rand() % 90;
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
#include "DBCore.h"
#include "buffer/SPSegment.h"
#include "utility/macros.h"
#include "utility/helpers.h"

#include "query/Register.h"
#include "query/TableScanOperator.h"

#include "gtest/gtest.h"

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
		std::string sql = std::string("create table dbtestA ( name char(50), age integer, somefield char(30), lastfield integer, primary key (lastfield));\n") +
			"create table dbtestB ( bestfield integer, age integer, bcharfield char(30), primary key (bestfield));";
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
	}
	virtual void TearDown() override
	{
		SDELETE( core );
	}
	DBCore* core;
	std::vector<std::string> nameA;
	std::vector<uint32_t> ageA;
	std::vector<std::string> somefieldA;
	std::vector<uint32_t> lastfieldA;

	std::vector<uint32_t> bestfieldB;
	std::vector<uint32_t> ageB;
	std::vector<std::string> bcharfieldB;

	Record GenerateRecordA()
	{
		std::vector<uint8_t> data;
		// name
		uint32_t n1 = rand() % firstNames.size();
		uint32_t n2 = rand() % lastNames.size();
		nameA.push_back( firstNames[n1] + " " + lastNames[n2] );
		AppendToData( firstNames[n1] + " " + lastNames[n2], data );

		// age
		uint32_t r = rand() % 90;
		ageA.push_back( r );
		AppendToData( r, data );

		// somefield
		std::string chosen = randomStrings[rand() % randomStrings.size()];
		somefieldA.push_back( chosen );
		AppendToData( chosen, data );

		// lastfield
		r = rand();
		ageA.push_back( r );
		AppendToData( r, data );

		return Record( static_cast<uint32_t>(data.size()), &data[0] );
	}
	Record GenerateRecordB()
	{
		std::vector<uint8_t> data;
		// bestfield
		uint32_t r = rand();
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
};

// There is a single test for every query directly after a table scan to test principal correctness
// In the end there is one big query chaining all the queries together
// TODO: Think about testing for updated/deleted table scan

// Test if the table scan works correctly
// This also tests correct setup of registers
TEST_F( QueryTest, TableScanQuery )
{
	TableScanOperator op( "dbtestA", *core, *core->GetBufferManager() );
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
		EXPECT_EQ( registers[0]->GetString(), nameA[curidx] );
		EXPECT_EQ( registers[1]->GetInteger(), ageA[curidx] );
		EXPECT_EQ( registers[2]->GetString(), somefieldA[curidx] );
		EXPECT_EQ( registers[3]->GetInteger(), lastfieldA[curidx] );
		++curidx;
	}
	op.Close();
}

#include "utility/defines.h"
#include "utility/macros.h"
#include "utility/helpers.h"

#include "DBCore.h"
#include "sql/Schema.h"
#include "buffer/SPSegment.h"

#include "query/Register.h"
#include "query/QueryOperator.h"
#include "query/TableScanOperator.h"
#include "query/PrintOperator.h"
#include "query/ProjectionOperator.h"
#include "query/SelectOperator.h"
#include "query/HashJoinOperator.h"

#include <iostream>
#include <sstream>
#include <string>
#include <stdint.h>
#include <assert.h>

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

Record GenerateRecordA()
{
	static uint32_t ctr = 0;
	std::vector<uint8_t> data;
	// name
	uint32_t n1 = rand() % firstNames.size();
	uint32_t n2 = rand() % lastNames.size();
	AppendToData( firstNames[n1] + " " + lastNames[n2], data );

	// age
	Integer r = rand() % 100;
	AppendToData( r, data );

	// somefield
	std::string chosen = randomStrings[rand() % randomStrings.size()];
	AppendToData( chosen, data );

	// lastfield
	r = ctr;
	AppendToData( r, data );

	++ctr;
	return Record( static_cast<uint32_t>(data.size()), &data[0] );
}
Record GenerateRecordB()
{
	std::vector<uint8_t> data;
	// bestfield
	Integer r = rand();
	AppendToData( r, data );

	// age
	r = rand() % 100;
	AppendToData( r, data );

	// bcharfield
	std::string chosen = randomStrings[rand() % randomStrings.size()];
	AppendToData( chosen, data );

	return Record( static_cast<uint32_t>(data.size()), &data[0] );
}

int main( int argc, char* argv[] )
{
	// Windows memory leak detection debug system call
#ifdef _CRTDBG_MAP_ALLOC
	_CrtSetDbgFlag( _CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF );
#endif // _CRTDBG_MAP_ALLOC

	DBCore core;
	core.WipeDatabase();
	std::string sql = std::string( "create table dbtestA ( name char(50), age integer, somefield char(30), lastfield integer, primary key (lastfield) );\n" ) +
		"create table dbtestB ( bestfield integer, age integer, bcharfield char(30), primary key (bestfield) );";
	core.AddRelationsFromString( sql );
	// Add a few hundred random entries to both relations so we can do some multipage queries
	std::unique_ptr<SPSegment> spA = core.GetSPSegment( "dbtestA" );
	for ( uint32_t i = 0; i < 2500; ++i )
	{
		spA->Insert( GenerateRecordA() );
	}
	std::unique_ptr<SPSegment> spB = core.GetSPSegment( "dbtestB" );
	for ( uint32_t i = 0; i < 2500; ++i )
	{
		spB->Insert( GenerateRecordB() );
	}

	TableScanOperator tsopA( "dbtestA", core, *core.GetBufferManager() );
	TableScanOperator tsopB( "dbtestB", core, *core.GetBufferManager() );
	SelectOperator sopA( tsopA, "name", "Arnold Schwarzenegger" );
	SelectOperator sopB( tsopB, "bcharfield", "YW6MWL1Ru9spoY" );
	HashJoinOperator hjop( sopA, sopB, "age", "age" ); // Join all same age pairs
	ProjectionOperator prop( hjop, std::vector<std::string>( { "name", "bcharfield", "age" } ) );
	// The query builds pairs of all Arnold Schwarzeneggers with the same age as whatever is in B with bcharfield YW6MWL1Ru9spoY
	PrintOperator printop( prop, std::cout );
	printop.Open();
	uint32_t ctr = 0;
	while (printop.Next())
	{
		++ctr;
	}
	printop.Close();
	std::cout << "Printed " << ctr << " tuples." << std::endl;
	return 0;
}
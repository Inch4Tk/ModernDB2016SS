
#include "utility/defines.h"
#include "utility/helpers.h"
#include "DBCore.h"
#include "sql/Schema.h"
#include <iostream>
#include <string>
#include <stdint.h>
#include <assert.h>

int main( int argc, char* argv[] )
{
	// Windows memory leak detection debug system call
#ifdef _CRTDBG_MAP_ALLOC
	_CrtSetDbgFlag( _CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF );
#endif // _CRTDBG_MAP_ALLOC

	if ( argc != 2 )
	{
		LogError( "usage: " + std::string( argv[0] ) + "<path_to_sql_file>");
		return -1;
	}
	DBCore core;
	/*core.AddRelationsFromString( std::string( "create table employee (id integer, country_id char( 2 ), mgr_id integer, " ) +
								 "salery integer, first_name char( 20 ), middle char( 1 ), last_name char( 20 )," +
								 "primary key( id, country_id ));\n" +
								 "create table country(country_id char( 2 ), short_name char( 20 ), long_name char( 50 ), primary key( country_id ));\n" +
								 "create table department(id integer, primary key( id ), name char( 25 ), country_id char( 2 ));"
								 );*/
	//core.WipeDatabase();
	/*std::vector<uint8_t> data;
	const_cast<Schema*>(core.GetSchema())->Serialize( data );
	Schema derp;
	derp.Deserialize( &data[0] );*/
	return 0;
}
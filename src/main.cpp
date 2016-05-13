
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
	return 0;
}

#include "utility/defines.h"
#include "utility/helpers.h"
#include "buffer/BufferManager.h"
#include "sql/SchemaParser.h"

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
	std::string sqlpath( argv[1] );
	SchemaParser parser( sqlpath );
	std::unique_ptr<Schema> schema = parser.parse();
	std::vector<uint8_t> data;
	schema->Serialize( data );
	return 0;
}
#include "helpers.h"

#include <iostream>
#include <fstream>
#include <string>
#include <stdint.h>
#include <assert.h>

int main( int argc, char* argv[] )
{
	//// Read console arguments
	//if ( argc < 4 )
	//{
	//	std::cerr << "usage: " << argv[0] << " <input file name> <output file name> <memory size in MB>" << std::endl;
	//	return -1;
	//}
	//int64_t memsize = atoll( argv[3] );
	//if ( memsize <= 0 )
	//{
	//	std::cerr << "invalid memory size: " << argv[3] << std::endl;
	//	return -1;
	//}

	//// We know memsize is bigger than 0 so we can cast
	//uint64_t memsizeMB = static_cast<uint64_t>(memsize);

	//// Call our external sorting algorithm
	//std::ifstream input( argv[1], std::ifstream::in | std::ifstream::binary | std::ifstream::ate );
	//if ( !input.is_open() )
	//{
	//	std::cerr << "Could not open input file: " << argv[1] << std::endl;
	//	return -1;
	//}
	//uint64_t size = input.tellg();
	//externalSort( argv[1], size, argv[2], memsizeMB * 1000000 );
	//std::cout << "Proceeding to check order of data..." << std::endl;
	//assertCorrectOrderSort( argv[2] );

	return 0;
}
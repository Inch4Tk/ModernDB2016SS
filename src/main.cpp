#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <stdint.h>


/// <summary>
/// Performs external merge sort on the input file and stores it into the output file.
/// </summary>
/// <param name="inputFilename">The input filename.</param>
/// <param name="size">The size of the data.</param>
/// <param name="outputFilename">The output filename.</param>
/// <param name="memsizeMB">The memsize in bytes</param>
void externalSort( const char* inputFilename, uint64_t size, const char* outputFilename, uint64_t memsize )
{
	// Number of uint64 in ram
	// Storing the number of data as uin32_t arguably makes more sense. 
	// If we wanted to use >32GB of data, we would have an overflow,
	// but in this case std::vector will overflow anyways.
	uint32_t numData = static_cast<uint32_t>(memsize / sizeof( uint64_t ));

	// Open input file in binary mode
	std::ifstream input;
	input.open( inputFilename, std::ifstream::in | std::ifstream::binary );

	if (!input.is_open())
	{
		std::cerr << "Could not open file: " << inputFilename << std::endl;
		return;
	}

	// Read first chunk
	uint32_t cur = 0;
	std::vector<uint64_t> data(numData);
	while ( input.good() && cur < numData )
	{
		// Read a single uint64 and insert into vector
		char tmp[sizeof( uint64_t )];
		input.read( tmp, sizeof( uint64_t ) );
		data[cur] = *reinterpret_cast<uint64_t*>(tmp);
		++cur;
	}

	// Close streams
	input.close();
}

int main( int argc, char* argv[] )
{
	// Read console arguments
	if ( argc < 4 )
	{
		std::cerr << "usage: " << argv[0] << " <input file name> <output file name> <memory size in MB>" << std::endl;
		return -1;
	}
	int64_t memsize = atoll( argv[3] );
	if ( memsize <= 0 )
	{
		std::cerr << "invalid memory size: " << argv[3] << std::endl;
		return -1;
	}

	// We know memsize is bigger than 0 so we can cast
	uint64_t memsizeMB = static_cast<uint64_t>(memsize);

	// Call our external sorting algorithm
	externalSort( argv[1], 0, argv[2], memsizeMB );

	return 0;
}
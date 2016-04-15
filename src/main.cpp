#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <stdint.h>


/// <summary>
/// Closes the temporary streams opened in the external sort.
/// </summary>
/// <param name="tmpstreams">The tmpstreams.</param>
void closeTempStreams( std::vector<std::ifstream>& tmpstreams )
{
	for ( std::ifstream& stream : tmpstreams )
	{
		if (stream.is_open())
		{
			stream.close();
		}
	}
}

/// <summary>
/// Cleans up the temporary files created in the external sort.
/// </summary>
/// <param name="tmpFilenames">The temporary filenames.</param>
void cleanupTempFiles( std::vector<std::string>& tmpFilenames )
{
	for ( std::string s : tmpFilenames )
	{
		std::remove( s.c_str() );
	}
}

/// <summary>
/// Performs external merge sort on the input file and stores it into the output file.
/// </summary>
/// <param name="inputFilename">The input filename.</param>
/// <param name="size">The size of the input data in bytes.</param>
/// <param name="outputFilename">The output filename.</param>
/// <param name="memsizeMB">The memsize in bytes</param>
void externalSort( const char* inputFilename, uint64_t size, const char* outputFilename, uint64_t memsize )
{
	// Number of uint64 in ram
	// Storing the number of data as uin32_t arguably makes more sense. 
	// If we wanted to use >32GB of data, we would have an overflow,
	// but in this case std::vector will overflow anyways.
	uint32_t chunkUints = static_cast<uint32_t>(memsize / sizeof( uint64_t ));
	uint32_t numChunks = static_cast<uint32_t>(size / memsize);
	if (size % memsize != 0)
		++numChunks;

	// Open input file in binary mode
	std::ifstream input;
	input.open( inputFilename, std::ifstream::in | std::ifstream::binary );
	if (!input.is_open())
	{
		std::cerr << "Could not open input file: " << inputFilename << std::endl;
		return;
	}

	// Read the data chunks, sort them, then store each into a temporary output file
	std::cout << "Start sorting chunks..." << std::endl;
	std::vector<std::string> tmpFilenames;
	for ( uint32_t curChunk = 0; curChunk < numChunks; ++curChunk )
	{
		// Read
		uint32_t curUint = 0;
		std::vector<uint64_t> data( chunkUints );
		while ( input.good() && curUint < chunkUints )
		{
			// Read a single uint64 and insert into vector
			char tmp[sizeof( uint64_t )];
			input.read( tmp, sizeof( uint64_t ) );
			data[curUint] = *reinterpret_cast<uint64_t*>(tmp);
			++curUint;
		}

		// Sort
		std::sort( data.begin(), data.end() );

		// Store
		std::ofstream tmpoutput;
		// Open temporary output file
		std::string outputName = std::string( outputFilename ) + "_tmp" + std::to_string( curChunk );
		tmpFilenames.push_back( outputName );
		tmpoutput.open( outputName, std::ofstream::out |
						std::ofstream::binary | std::ofstream::trunc );
		if ( !tmpoutput.is_open() || tmpoutput.fail() )
		{
			std::cerr << "Could not open temporary chunk file: " << outputName << std::endl;
			cleanupTempFiles( tmpFilenames );
			return;
		}

		// Write data
		tmpoutput.write( reinterpret_cast<const char*>(&data[0]), chunkUints * sizeof( uint64_t ) );
		if ( !tmpoutput.good() )
		{
			std::cerr << "An error occurred writing to " << outputName << std::endl;
			tmpoutput.close();
			cleanupTempFiles( tmpFilenames );
			return;
		}

		// Close tmp chunk streams
		tmpoutput.close();
		std::cout << "Finished sorting chunk " << curChunk << " of " << numChunks << " chunks" << std::endl;
	}
	// Close input stream
	input.close();

	// K-way merge
	std::cout << "Start " << numChunks << "-way merge of chunks" << std::endl;

	// Open the output stream
	std::ofstream outputStream;
	outputStream.open( outputFilename, std::ofstream::out |
					   std::ofstream::binary | std::ofstream::trunc );
	if ( !outputStream.is_open() )
	{
		std::cerr << "Could not open output file: " << outputFilename << std::endl;
		cleanupTempFiles( tmpFilenames );
		return;
	}

	// Open all the temp chunk streams
	std::vector<std::ifstream> chunkStreams;
	for ( std::string tmpChunkFile : tmpFilenames )
	{
		chunkStreams.push_back( std::ifstream( tmpChunkFile, std::ifstream::in | std::ifstream::binary ) );
		if ( !chunkStreams.back().is_open() )
		{
			std::cerr << "Could not open temporary chunk file: " << tmpChunkFile << std::endl;
			outputStream.close();
			closeTempStreams( chunkStreams );
			cleanupTempFiles( tmpFilenames );
			return;
		}
	}

	// Load small chunks of data and perform merges


	// Close streams
	outputStream.close();
	closeTempStreams( chunkStreams );

	// Clean up temporary files
	cleanupTempFiles( tmpFilenames );
	std::cout << "Finished and cleaned up, you can find the sorted data in: " << inputFilename << std::endl;
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
	std::ifstream input( argv[1], std::ifstream::in | std::ifstream::binary | std::ifstream::ate );
	if ( !input.is_open() )
	{
		std::cerr << "Could not open input file: " << argv[1] << std::endl;
		return -1;
	}
	uint64_t size = input.tellg();
	externalSort( argv[1], size, argv[2], memsizeMB * 1000000 );

	return 0;
}
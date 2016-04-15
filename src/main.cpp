#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <queue>
#include <algorithm>
#include <stdint.h>
#include <assert.h>

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
/// Loads data for the chunk from a temporary file.
/// </summary>
/// <param name="chunkStream">The chunk stream.</param>
/// <param name="target">The target.</param>
/// <param name="amount">The amount of uints to load.</param>
/// <returns>Success</returns>
bool loadChunk( std::ifstream& chunkStream, std::queue<uint64_t>& target, uint32_t amount )
{
	uint32_t cur = 0;
	while ( chunkStream.good() && cur < amount )
	{
		// Read a single uint64 and insert into vector
		char tmp[sizeof( uint64_t )];
		chunkStream.read( tmp, sizeof( uint64_t ) );
		if ( !chunkStream.fail() )
		{
			target.push( *reinterpret_cast<uint64_t*>(tmp) );
		}
		++cur;
	}
	if ( chunkStream.fail() && !chunkStream.eof() )
	{
		return false;
	}
	return true;
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
		std::vector<uint64_t> data;
		while ( input.good() && curUint < chunkUints )
		{
			// Read a single uint64 and insert into vector
			char tmp[sizeof( uint64_t )];
			input.read( tmp, sizeof( uint64_t ) );
			if ( !input.fail() )
			{
				data.push_back( *reinterpret_cast<uint64_t*>(tmp) );
			}
			++curUint;
		}
		if ( input.fail() && !input.eof() )
		{
			std::cerr << "Error occurred while reading form input " << inputFilename << std::endl;
			input.close();
			cleanupTempFiles( tmpFilenames );
			return;
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
			input.close();
			cleanupTempFiles( tmpFilenames );
			return;
		}

		// Write data
		tmpoutput.write( reinterpret_cast<const char*>(&data[0]), data.size() * sizeof( uint64_t ) );
		if ( tmpoutput.fail() )
		{
			std::cerr << "An error occurred writing to " << outputName << std::endl;
			input.close();
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
	// Some design thoughts:
	// Priority Q push is logarithmic in the number of elements n currently contained in the prio Q (up to chunk size)
	// Making a test for the smallest number in each chunk is linear in number of chunks k
	// As long as the k < log2 n we should (in theory) not lose insertion speed
	// 5GB Data ~ 625 Mio uint64. Memsize 256 MB => 20 Chunks (k). Average prio q size = 0.5 * max chunk size = 15.6 Mio uints. log2 15.6 Mio (n) ~ 24
	// 5GB Data ~ 625 Mio uint64. Memsize 128 MB => 40 Chunks (k). Average prio q size = 0.5 * max chunk size = 7.8 Mio uints. log2 7.8 Mio (n) ~ 23
	// 500MB Data ~ 62.5 Mio uint64. Memsize 128 MB => 4 Chunks (k). Average prio q size = 0.5 * max chunk size = 15.6 Mio uints. log2 15.6 Mio (n) ~ 23
	// Bigger data and correspondingly smaller memory -> Priority Q performs better. 
	// Smaller data to correspondingly bigger memory -> Comparing performs better.
	// However, if we have a big amount of chunks we should consider doing a multipass merge anyways to reduce hard drive writes.
	// Of course actual speed data varies on the implementations of priority queues and on data sizes/memory sizes.
	// However, the implementation of comparing should be easier, since we do not have to watch for corner cases when we 
	// store our output buffer to the file (with prio queues we have to first insert all elements still left in the chunks 
	// that are smaller than the biggest element currently in the queue, before saving data).
	std::vector<uint64_t> outData;
	std::vector<std::queue<uint64_t>> chunkData( chunkStreams.size() );
	uint32_t smallChunkUints = chunkUints / numChunks;
	while ( true )
	{
		// Go over all chunks and select the chunk with the smallest number
		bool allChunksEmpty = true;
		uint64_t curSmallest = UINT64_MAX;
		uint32_t smallestIdx = 0;
		uint32_t idx = 0;
		for ( std::queue<uint64_t>& cd : chunkData )
		{
			if ( cd.empty() && !chunkStreams[idx].eof() )
			{
				bool success = loadChunk( chunkStreams[idx], cd, smallChunkUints );
				if ( !success )
				{
					std::cerr << "An error occurred reading from temporary chunk file " << tmpFilenames[idx] << std::endl;
					outputStream.close();
					closeTempStreams( chunkStreams );
					cleanupTempFiles( tmpFilenames );
					return;
				}
			}

			if ( !cd.empty() && cd.front() < curSmallest )
			{
				smallestIdx = idx;
				curSmallest = cd.front();
			}
			allChunksEmpty = allChunksEmpty && cd.empty() && chunkStreams[idx].eof(); // Set empty bit if we are empty
			++idx;
		}

		// Make a check if all chunks are empty and terminate
		if ( allChunksEmpty )
		{
			if ( outData.size() > 0 )
			{
				outputStream.write( reinterpret_cast<const char*>(&outData[0]), outData.size() * sizeof( uint64_t ) );
			}
			if ( outputStream.fail() )
			{
				std::cerr << "An error occurred writing to " << outputFilename << std::endl;
				outputStream.close();
				closeTempStreams( chunkStreams );
				cleanupTempFiles( tmpFilenames );
				return;
			}
			outData.clear();
			break;
		}

		// Insert smallest number into output and remove corresponding chunk
		outData.push_back( curSmallest );
		chunkData[smallestIdx].pop();

		// Write back output buffer if it is full
		if (outData.size() >= smallChunkUints )
		{
			outputStream.write( reinterpret_cast<const char*>(&outData[0]), outData.size() * sizeof( uint64_t ) );
			if ( outputStream.fail() )
			{
				std::cerr << "An error occurred writing to " << outputFilename << std::endl;
				outputStream.close();
				closeTempStreams( chunkStreams );
				cleanupTempFiles( tmpFilenames );
				return;
			}
			outData.clear();
		}
	}

	// Close streams
	outputStream.close();
	closeTempStreams( chunkStreams );

	// Clean up temporary files
	cleanupTempFiles( tmpFilenames );
	std::cout << "Finished and cleaned up, you can find the sorted data in: " << outputFilename << std::endl;
}

/// <summary>
/// Asserts the correct order of numbers in the output file.
/// </summary>
/// <param name="outputFilename">The output filename.</param>
void assertCorrectOrder( const char* outputFilename )
{
	uint64_t lastNumber = 0;
	// Open output file in binary mode
	std::ifstream output;
	output.open( outputFilename, std::ifstream::in | std::ifstream::binary );
	if ( !output.is_open() )
	{
		std::cerr << "Could not open output file: " << outputFilename << std::endl;
		return;
	}

	while ( output.good() )
	{
		// Read a single uint64 and insert into vector
		char tmp[sizeof( uint64_t )];
		output.read( tmp, sizeof( uint64_t ) );
		if ( !output.fail() )
		{
			uint64_t nextNumber = *reinterpret_cast<uint64_t*>(tmp);
			assert( nextNumber >= lastNumber ); // Just for nice exceptions
			if (nextNumber < lastNumber)
			{
				std::cerr << "Wrong order in output file detected, " << nextNumber << " is smaller than " << lastNumber << std::endl;
				output.close();
				return;
			}
			lastNumber = nextNumber;
		}
	}
	std::cout << "No wrong order in output file detected!" << std::endl;
	output.close();
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
	assertCorrectOrder( argv[2] );

	return 0;
}
#include "helpers.h"

#include "utility/defines.h"

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <queue>
#include <algorithm>
#include <assert.h>

#ifdef PLATFORM_UNIX      
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#elif defined(PLATFORM_WIN)
#include <windows.h>
#endif


//////////////////////////////////////////////////////////////////////////
/// Locally Available helpers/functions
//////////////////////////////////////////////////////////////////////////

/// <summary>
/// Closes the temporary streams opened in the external sort.
/// </summary>
/// <param name="tmpstreams">The tmpstreams.</param>
void CloseTempStreams( std::vector<std::ifstream*>& tmpstreams )
{
	for ( std::ifstream* stream : tmpstreams )
	{
		if ( stream->is_open() )
		{
			stream->close();
		}
		delete stream;
	}
	tmpstreams.clear();
}

/// <summary>
/// Cleans up the temporary files created in the external sort.
/// </summary>
/// <param name="tmpFilenames">The temporary filenames.</param>
void CleanupTempFiles( std::vector<std::string>& tmpFilenames )
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
bool LoadChunk( std::ifstream* chunkStream, std::queue<uint64_t>& target, uint32_t amount )
{
	uint32_t cur = 0;
	while ( chunkStream->good() && cur < amount )
	{
		// Read a single uint64 and insert into vector
		uint64_t tmp;
		chunkStream->read( reinterpret_cast<char*>(&tmp), sizeof( uint64_t ) );
		if ( !chunkStream->fail() )
		{
			target.push( tmp );
		}
		++cur;
	}
	if ( chunkStream->fail() && !chunkStream->eof() )
	{
		return false;
	}
	return true;
}

//////////////////////////////////////////////////////////////////////////
/// Globally Available helpers/functions
//////////////////////////////////////////////////////////////////////////

/// <summary>
/// Performs external merge sort on the input file and stores it into the output file.
/// </summary>
/// <param name="inputFilename">The input filename.</param>
/// <param name="size">The size of the input data in bytes.</param>
/// <param name="outputFilename">The output filename.</param>
/// <param name="memsizeMB">The memsize in bytes</param>
void ExternalSort( const char* inputFilename, uint64_t size, const char* outputFilename, uint64_t memsize )
{
	// Number of uint64 in ram
	// Storing the number of data as uin32_t arguably makes more sense. 
	// If we wanted to use >32GB of data, we would have an overflow,
	// but in this case std::vector might overflow anyways (depending on system/implementation)
	assert( memsize / sizeof( uint64_t ) <= static_cast<uint64_t>(UINT32_MAX) );
	uint32_t chunkUints = static_cast<uint32_t>(memsize / sizeof( uint64_t ));
	uint32_t numChunks = static_cast<uint32_t>(size / memsize);
	if ( size % memsize != 0 )
		++numChunks;

	// Open input file in binary mode
	std::ifstream input;
	input.open( inputFilename, std::ifstream::in | std::ifstream::binary );
	if ( !input.is_open() )
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
			uint64_t tmp;
			input.read( reinterpret_cast<char*>(&tmp), sizeof( uint64_t ) );
			if ( !input.fail() )
			{
				data.push_back( tmp );
			}
			++curUint;
		}
		if ( input.fail() && !input.eof() )
		{
			std::cerr << "Error occurred while reading form input " << inputFilename << std::endl;
			input.close();
			CleanupTempFiles( tmpFilenames );
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
			std::cerr << "Could not open temporary chunk file: " << 
				outputName << std::endl;
			input.close();
			CleanupTempFiles( tmpFilenames );
			return;
		}

		// Write data
		tmpoutput.write( reinterpret_cast<const char*>(&data[0]), 
						 data.size() * sizeof( uint64_t ) );
		if ( tmpoutput.fail() )
		{
			std::cerr << "An error occurred writing to " << outputName << std::endl;
			input.close();
			tmpoutput.close();
			CleanupTempFiles( tmpFilenames );
			return;
		}

		// Close tmp chunk streams
		tmpoutput.close();
		std::cout << "Finished sorting chunk " << curChunk + 1 << " of " << 
			numChunks << " chunks" << std::endl;
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
		std::cerr << "Could not open output file: " << 
			outputFilename << std::endl;
		CleanupTempFiles( tmpFilenames );
		return;
	}

	// Open all the temp chunk streams
	std::vector<std::ifstream*> chunkStreams;
	for ( std::string tmpChunkFile : tmpFilenames )
	{
		std::ifstream* stream = new std::ifstream( tmpChunkFile, 
												   std::ifstream::in | std::ifstream::binary );
		chunkStreams.push_back( stream );
		if ( !chunkStreams.back()->is_open() )
		{
			std::cerr << "Could not open temporary chunk file: " << 
				tmpChunkFile << std::endl;
			outputStream.close();
			CloseTempStreams( chunkStreams );
			CleanupTempFiles( tmpFilenames );
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
			if ( cd.empty() && !chunkStreams[idx]->eof() )
			{
				bool success = LoadChunk( chunkStreams[idx], cd, smallChunkUints );
				if ( !success )
				{
					std::cerr << "An error occurred reading from temporary chunk file " << 
						tmpFilenames[idx] << std::endl;
					outputStream.close();
					CloseTempStreams( chunkStreams );
					CleanupTempFiles( tmpFilenames );
					return;
				}
			}

			if ( !cd.empty() && cd.front() < curSmallest )
			{
				smallestIdx = idx;
				curSmallest = cd.front();
			}
			allChunksEmpty = allChunksEmpty && cd.empty() && 
				chunkStreams[idx]->eof(); // Set empty bit if we are empty
			++idx;
		}

		// Make a check if all chunks are empty and terminate
		if ( allChunksEmpty )
		{
			if ( outData.size() > 0 )
			{
				outputStream.write( reinterpret_cast<const char*>(&outData[0]), 
									outData.size() * sizeof( uint64_t ) );
			}
			if ( outputStream.fail() )
			{
				std::cerr << "An error occurred writing to " << 
					outputFilename << std::endl;
				outputStream.close();
				CloseTempStreams( chunkStreams );
				CleanupTempFiles( tmpFilenames );
				return;
			}
			outData.clear();
			break;
		}

		// Insert smallest number into output and remove corresponding chunk
		outData.push_back( curSmallest );
		chunkData[smallestIdx].pop();

		// Write back output buffer if it is full
		if ( outData.size() >= smallChunkUints )
		{
			outputStream.write( reinterpret_cast<const char*>(&outData[0]), 
								outData.size() * sizeof( uint64_t ) );
			if ( outputStream.fail() )
			{
				std::cerr << "An error occurred writing to " << 
					outputFilename << std::endl;
				outputStream.close();
				CloseTempStreams( chunkStreams );
				CleanupTempFiles( tmpFilenames );
				return;
			}
			outData.clear();
		}
	}

	// Close streams
	outputStream.close();
	CloseTempStreams( chunkStreams );

	// Clean up temporary files
	CleanupTempFiles( tmpFilenames );
	std::cout << "Finished and cleaned up, you can find the sorted data in: " << 
		outputFilename << std::endl;
}

/// <summary>
/// Asserts the correct order of numbers in the output file.
/// </summary>
/// <param name="outputFilename">The output filename.</param>
void AssertCorrectOrderSort( const char* outputFilename )
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
		uint64_t nextNumber;
		output.read( reinterpret_cast<char*>(&nextNumber), sizeof( uint64_t ) );
		if ( !output.fail() )
		{
			// Make checks
			assert( nextNumber >= lastNumber ); // Just for nice exceptions
			if ( nextNumber < lastNumber )
			{
				std::cerr << "Wrong order in output file detected, " << 
					nextNumber << " is smaller than " << lastNumber << std::endl;
				output.close();
				return;
			}
			lastNumber = nextNumber;
		}
	}
	std::cout << "No wrong order in output file detected!" << std::endl;
	output.close();
}

/// <summary>
/// Logs something only in debug compilation mode. Log output goes to std::cout.
/// </summary>
/// <param name="debugMessage">The debug message.</param>
void LogDebug( const std::string& debugMessage )
{
#ifndef NDEBUG
	LogDebug( std::wstring( debugMessage.begin(), debugMessage.end() ) );
#endif
}

/// <summary>
/// Logs something only in debug compilation mode. Log output goes to std::cout.
/// </summary>
/// <param name="debugMessage">The debug message.</param>
void LogDebug( const std::wstring& debugMessage )
{
#ifndef NDEBUG
	std::wcout << "DebugInfo: " << debugMessage << std::endl;
#endif
}

/// <summary>
/// Logs an error. Log output goes to std::cerr.
/// </summary>
/// <param name="errorMessage">The error message.</param>
void LogError( const std::string& errorMessage )
{
	LogError( std::wstring( errorMessage.begin(), errorMessage.end() ) );
}

/// <summary>
/// Logs an error. Log output goes to std::cerr.
/// </summary>
/// <param name="errorMessage">The error message.</param>
void LogError( const std::wstring& errorMessage )
{
	std::wcerr << "Error: " << errorMessage << std::endl;
}

/// <summary>
/// Logs the specified message, use this if the message should appear in Release mode. Log output goes to std::cout.
/// </summary>
/// <param name="message">The message.</param>
void Log( const std::string& message )
{
	Log( std::wstring( message.begin(), message.end() ) );
}

/// <summary>
/// Logs the specified message, use this if the message should appear in Release mode. Log output goes to std::cout.
/// </summary>
/// <param name="message">The message.</param>
void Log( const std::wstring& message )
{
	std::wcout << message << std::endl;
}

/// <summary>
/// Cross platform check if file exists.
/// </summary>
/// <param name="filename">The filename.</param>
/// <returns></returns>
bool FileExists( const std::string& filename )
{
#ifdef PLATFORM_UNIX
	struct stat buffer;
	return (stat( filename.c_str(), &buffer ) == 0);
#elif defined(PLATFORM_WIN)
	DWORD dwAttrib = GetFileAttributes( filename.c_str() );
	return (dwAttrib != INVALID_FILE_ATTRIBUTES && 
			!(dwAttrib & FILE_ATTRIBUTE_DIRECTORY));
#else
	LogError( "Could not detect a valid platform!" );
	throw std::runtime_error("Error: Detect Platform");
	return false;
#endif
}

/// <summary>
/// Deletes a file
/// </summary>
/// <param name="filename">The filename.</param>
void FileDelete( const std::string& filename )
{
	std::remove( filename.c_str() );
}

/// <summary>
/// Splits the tuple id.
/// </summary>
/// <param name="toSplit">To split.</param>
/// <returns>First uint64 is the page id, second uint64 is the page id. Correctly shifted to the least significant bits</returns>
std::pair<uint64_t, uint64_t> SplitTID( TID toSplit )
{
	uint64_t pageId = (toSplit & 0xFFFFFFFFFFFF0000ul) >> 16;
	assert( pageId <= 0xFFFFFFFFFFFFul );
	uint64_t slotId = toSplit & 0x000000000000FFFFul;
	assert( slotId <= 0xFFFFul );
	return std::make_pair( pageId, slotId );
}

/// <summary>
/// Merges the tuple id.
/// </summary>
/// <param name="pageId">The page identifier. Ignores the first 16 bits.</param>
/// <param name="slotId">The slot identifier. Ignores the first 48 bits.</param>
/// <returns>First 48 bit (starting from most significant bit) will be the page id. Last 16 bit the slot id.</returns>
TID MergeTID( uint64_t pageId, uint64_t slotId )
{
	pageId = (pageId & 0x0000FFFFFFFFFFFFul) << 16;
	slotId = slotId & 0x000000000000FFFFul;
	return pageId | slotId;
}

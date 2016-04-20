#include "BufferManager.h"

#include "BufferFrame.h"
#include "helpers.h"
#include "defines.h"

#include <string>
#include <fstream>
#include <exception>
#include <assert.h>

/// <summary>
/// Initializes a new instance of the <see cref="BufferManager" /> class.
/// </summary>
/// <param name="pageCount">The page count.</param>
BufferManager::BufferManager( uint32_t pageCount ) : mPageCount( pageCount )
{
}

/// <summary>
/// Finalizes an instance of the <see cref="BufferManager"/> class. Write all dirty frames to disk and free all resources.
/// </summary>
BufferManager::~BufferManager()
{

}

/// <summary>
/// Fixes the page. // TODO More doc
/// </summary>
/// <param name="pageId">The page identifier.</param>
/// <param name="exclusive">if set to <c>true</c> [exclusive].</param>
/// <returns></returns>
BufferFrame& BufferManager::FixPage( uint64_t pageId, bool exclusive )
{
	return BufferFrame();
}

/// <summary>
/// Unfixes the page. // TODO More doc
/// </summary>
/// <param name="frame">The frame.</param>
/// <param name="isDirty">if set to <c>true</c> [is dirty].</param>
void BufferManager::UnfixPage( BufferFrame& frame, bool isDirty )
{

}

/// <summary>
/// Loads a page from harddrive.
/// </summary>
/// <param name="pageId">The page identifier.</param>
void BufferManager::LoadPage( BufferFrame& frame )
{
	auto ids = SplitPageId( frame.GetPageId() );
	// Open page file in binary mode
	std::string segmentName = std::to_string( ids.first );
	std::ifstream segment;
	segment.open( segmentName, std::ifstream::in | std::ifstream::binary );
	if ( !segment.is_open() )
	{
		LogError( "Failed to open segment file " + segmentName );
		throw std::exception();
	}

	// Read data from input file
	uint64_t pos = ids.second * DB_PAGE_SIZE;
	segment.seekg( pos );
	segment.read( reinterpret_cast<char*>(frame.GetData()), DB_PAGE_SIZE );

	// Check for any errors reading
	if ( segment.fail() )
	{
		LogError( "Read error in segment " + segmentName );
		throw std::exception();
	}

	// Cleanup
	segment.close();
}

/// <summary>
/// Writes a page to harddrive.
/// </summary>
/// <param name="pageId">The page identifier.</param>
void BufferManager::WritePage( BufferFrame& frame )
{
	// TODO: Think about checking for fixed/exclusiveness

	auto ids = SplitPageId( frame.GetPageId() );
	// Open page file in binary mode
	std::string segmentName = std::to_string( ids.first );
	std::ofstream segment;
	segment.open( segmentName, std::ifstream::out | std::ifstream::binary );
	if ( !segment.is_open() )
	{
		LogError( "Failed to open segment file " + segmentName );
		throw std::exception();
	}

	// Find position in output file
	uint64_t pos = ids.second * DB_PAGE_SIZE;
	segment.seekp( pos );
	segment.write( reinterpret_cast<char*>(frame.GetData()), DB_PAGE_SIZE );

	// Check for any errors reading
	if ( segment.fail() )
	{
		LogError( "Write error in segment " + segmentName + " on page " + std::to_string( ids.second ) );
		throw std::exception();
	}

	// Remove dirty flag from frame
	frame.mDirty = false;

	// Cleanup
	segment.close();
}

/// <summary>
/// Splits the page identifier into SegmentId and PageId
/// </summary>
/// <param name="pageId">The page identifier.</param>
/// <returns></returns>
std::pair<uint64_t, uint64_t> BufferManager::SplitPageId( uint64_t pageId )
{
	uint64_t segmentId = (pageId & 0xFFFF000000000000ul) >> 48;
	assert( segmentId <= 0xFFFFul );
	uint64_t nPageId = pageId & 0x0000FFFFFFFFFFFFul;
	assert( segmentId <= 0xFFFFFFFFFFFFul );
	return std::make_pair( segmentId, nPageId );
}

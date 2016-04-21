#include "BufferManager.h"

#include "utility/helpers.h"
#include "utility/defines.h"
#include "utility/macros.h"

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
	// Create and allocate huge chunk of consecutive memory
	mBufferMemory = new uint8_t[pageCount * DB_PAGE_SIZE];
	// Create buffer frames that divide up the memory
	mFrames.resize( pageCount );
	for ( uint32_t i = 0; i < mFrames.size(); ++i)
	{
		mFrames[i].mData = mBufferMemory + i * DB_PAGE_SIZE;
	}
}

/// <summary>
/// Finalizes an instance of the <see cref="BufferManager"/> class. Write all dirty frames to disk and free all resources.
/// </summary>
BufferManager::~BufferManager()
{
	// TODO: Think about a way to handle still fixed frames
	// Write all dirty frames back to disc
	for ( BufferFrame& f : mFrames )
	{
		if ( f.IsDirty() )
		{
			try // Should one write crash, try to still write back as much of the rest as possible
			{
				WritePage( f );
			}
			catch (std::runtime_error& e)
			{
				LogError( e.what() );
				LogError( "Caught runtime exception while writing back dirty files on shutdown. (PageId: " + 
						  std::to_string( f.GetPageId() ) + ")" );
			}
		}
	}
	mFrames.clear();

	ADELETE( mBufferMemory );
}

/// <summary>
/// Fixes the page. // TODO More doc
/// </summary>
/// <param name="pageId">The page identifier.</param>
/// <param name="exclusive">if set to <c>true</c> [exclusive].</param>
/// <returns></returns>
BufferFrame& BufferManager::FixPage( uint64_t pageId, bool exclusive )
{
	// Fixing a page:
	// - Acquire read lock on hash map
	// - Search for page
	// - Release read lock on hash map
	// - If we found page acquire desired lock on page and change needed page atomic values (finished)
	// - Else: Start replacement/loading process
	// - Find free page with Page Replacement Algorithm (1)
	// - Acquire write lock on hash map
	//    (Locking hash map, why?: std::unordered_map is not guaranteed to be thread safe on writes)
	// - Try acquire write lock on page: if not possible, it just got snatched away we then release hash map lock and go back to (1)
	//    Reason: We can not acquire the lock on the page first, because then somebody might just try to access it before we lock the hash map
	//    we would then proceed to remove this page from the map, and exchange it with something else, but the other access would
	//    still expect to receive the old page after we release our lock and we have a big problem.
	// - Remove found free page from hash map
	// - Release write lock on hash map
	// - Flush page if dirty
	// - Load the new page
	// - Acquire write exclusive lock on hash map
	// - Insert page
	// - Release write exclusive lock on hash map
	// - If we just need read lock on page, release write lock and acquire read lock
	// Unfixing a page:
	// - Update needed page atomics
	// - Release lock on page
	
	// Page Replacement Algorithm: Modified Second Chance
	// - Maintain counter in every BufferFrame. Starting at 0.
	// - Every time a page is unfixed increment counter by 1.
	// - If the page is dirty, increment counter by 2 instead.
	// To Find a page:
	// - Cyclic walk over all pages (ignore fixed pages)
	// - If page has a 0 counter, break and replace this page
	// - Integer-Divide every counter by 2 after inspection
	// - Remember page with smallest counter
	// - If we could not find an unfixed page after a full cycle we fail
	// - If we could not find a page with 0 counter, we free smallest counter page
	// - If this page is not unfixed anymore, we restart the search 
	//   (this round has a good chance to find something with 0 counter)
	// Dealing with concurrency:
	// - Counters are atomic
	// - Fixed pages are secured by rw mutex (so we just check if unlocked)

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
	// TODO: Think about checking for fixed/exclusiveness

	auto ids = SplitPageId( frame.GetPageId() );

	// Open page file in binary mode
	std::string segmentName = std::to_string( ids.first );
	if (!FileExists(segmentName))
	{
		// The segment does not yet exist, we just pretend we loaded the page,
		// this is valid behavior since there is no data yet.
		memset( frame.mData, 0, DB_PAGE_SIZE ); // Zero out the memory
		frame.mLoaded = true;
		return;
	}

	std::ifstream segment;
	segment.open( segmentName, std::ifstream::in | std::ifstream::binary );
	if ( !segment.is_open() )
	{
		LogError( "Failed to open segment file " + segmentName );
		throw std::runtime_error( "Error: Opening File" );
	}

	// Read data from input file
	uint64_t pos = ids.second * DB_PAGE_SIZE;
	segment.seekg( pos );
	segment.read( reinterpret_cast<char*>(frame.mData), DB_PAGE_SIZE );

	// Check for any errors reading (ignore eof errors, since these are normal if the page was not even created yet)
	if ( segment.fail() && !segment.eof() )
	{
		LogError( "Read error in segment " + segmentName + " on page " + std::to_string( ids.second ) );
		throw std::runtime_error( "Error: Reading File" );
	}

	// Set loaded bit
	frame.mLoaded = true;

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
		throw std::runtime_error( "Error: Opening File" );
	}

	// Find position in output file
	uint64_t pos = ids.second * DB_PAGE_SIZE;
	segment.seekp( pos );
	segment.write( reinterpret_cast<char*>(frame.mData), DB_PAGE_SIZE );

	// Check for any errors reading
	if ( segment.fail() )
	{
		LogError( "Write error in segment " + segmentName + " on page " + std::to_string( ids.second ) );
		throw std::runtime_error( "Error: Writing File" );
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

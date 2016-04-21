#include "BufferManager.h"

#include "utility/helpers.h"
#include "utility/defines.h"
#include "utility/macros.h"

#include <stdio.h>
#include <assert.h>
#include <cstring>
#include <string>
#include <fstream>
#include <exception>
#include <atomic>

//////////////////////////////////////////////////////////////////////////
// Overview (Thinking things through)
//////////////////////////////////////////////////////////////////////////
// Fixing a page:
// - Acquire read lock on hash map
// - Search for page
// - Release read lock on hash map
// - If: Found page, acquire desired lock on page and change needed page atomic/non-atomic values (finished)
//   Note: After acquiring our desired lock we must check if it is still the expected page. This is necessary so we can optimize
//         the write lock times on the whole map during the replacement process and never get into a situation where the following happens:
//         - Thread 1 finds page x in hash map and tries to acquire lock
//         - Thread 2 determines to replace page x and acquires lock before Thread 1
//         - Page x gets replaced and Thread 2 proceeds with his work and releases the lock
//         - Thread 1 acquires the lock, but the page is not the page he expects anymore
//         The two ways that can prevent this are:
//         (a) Acquire a write (exclusive) lock on the map before trying to write lock the page. If page lock fails
//             another thread got to it before us. To not block the hash map we release our hash lock and try with another page.
//             This way, we might need to make multiple write locks on the hash map (we also need to perform 1 lock extra operation
//             while we locked the hash map).
//         (b) Acquire write lock on page before locking the map. This makes sure we can replace the page once we lock the map.
//             But the scenario described above can happen, therefore threads have to verify they got the correct page before using it.
//             The good thing, we can do this in FixPage, the user does not have to change his behavior.
//         Since acquiring a write lock on the hash map is a really expensive operation (blocks all further parallel access from the
//         time it is called), this should be done as few times as possible and should be finished swiftly.
// - Else: Start replacement/loading process
// - Find free page with Page Replacement Algorithm
// - Acquire write lock on page
// - Flush page if dirty
// - Load the new page
// - Acquire write exclusive lock on hash map
// - Remove old page entry in hash map
//   Note: This is kind of a micro optimization, which I am not 100% sure about. We could instead remove the page
//         as soon as we lock it. This would need another cycle of hash map write lock/unlock.
//         Waiting until now means, that other threads can find this page for a much longer time under its old id
//         in the hash map. They will then try to acquire locks and wait for a long time. Once they end up in the lock
//         possession, they will find out it is not their desired page and have then also have to reload the page from disk
//         In these cases they will have a much longer wait time. The upside is that we can reduce the number of total
//         lockdowns on the hash map by 50%. If we assume that the above case happens very rarely (that an evicted page
//         gets loaded in the few extra milliseconds it will still be in the map), the micro optimization should yield
//         good results on average.
// - Insert new page entry
// - Release write exclusive lock on hash map
// - If we just need read lock on page, release write lock and acquire read lock
//////////////////////////////////////////////////////////////////////////
// Unfixing a page:
// - Update needed page atomics
// - Release lock on page
//////////////////////////////////////////////////////////////////////////
// Page Replacement Algorithm: Modified Second Chance
// - Maintain counter in every BufferFrame. Starting at 0. 
//   (We could also consider starting at uint64_max, so our first unfixing does not save the page)
// - Every time a page is unfixed increment counter by 1.
// - If the page is dirty, increment counter by 2 instead.
// To Find a page:
// - Cyclic walk over all pages (ignore fixed pages)
// - If page has a 0 counter, break and replace this page
// - Integer-Divide every counter by 2 after inspection
// - If we could not find any unfixed page after 2 * pagecount tries we throw
// - If there are unfixed pages, we continue until one page counter is 0
// Dealing with concurrency:
// - Counters are atomic
// Other notes:
// - As an optimization do not acquire locks to check if pages are unfixed while walking,
//   just use a more unreliable query, and try again if we fail to acquire a lock afterwards.
// - Cyclic walks will skip elements in a concurrent situation, so no page replaces try for
//   the same element. This might also mean, that in very very rare cases we throw, even though
//   there are still a few unfixed pages left over. (Only happens if a lot of concurrent replaces
//   happen and almost all pages are fixed)

/// <summary>
/// Initializes a new instance of the <see cref="BufferManager" /> class.
/// </summary>
/// <param name="pageCount">The page count.</param>
BufferManager::BufferManager( uint32_t pageCount ) : mPageCount( pageCount )
{
	assert( pageCount != 0 );
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
				LogError( "Caught runtime exception while writing " +
						  std::string("back dirty files on shutdown. (PageId: ") + 
						  std::to_string( f.GetPageId() ) + ")" );
			}
		}
	}
	mFrames.clear();

	ADELETE( mBufferMemory );
}

/// <summary>
/// Fixes the page. // TODO More doc .... throws on fail
/// </summary>
/// <param name="pageId">The page identifier.</param>
/// <param name="exclusive">if set to <c>true</c> [exclusive].</param>
/// <returns></returns>
BufferFrame& BufferManager::FixPage( uint64_t pageId, bool exclusive )
{
	// For a full explanation of the method see overview at the beginning of the file
	BufferFrame* frame = nullptr;
	// Compress the lock as much as possible, by releasing as soon as we get
	// a pointer to our object (if that exists)
	mHashMapLock.LockRead();
	auto it = mLoadedFrames.find( pageId );
	if ( it != mLoadedFrames.end() )
	{
		frame = it->second;
	}
	mHashMapLock.UnlockRead();

	if (frame)
	{
		// We found the page, try to acquire our desired lock
		frame->Lock( exclusive );
		// If our page is not the correct page, we release the lock and do a recursive call to fix page
		// (Reason is explained in overview)
		if (frame->GetPageId() != pageId)
		{
			frame->Unlock();
			return FixPage( pageId, exclusive );
		}
	}
	else
	{
		// We did not find the page, start replacement algorithm
		do 
		{
			frame = FindReplacementPage();
			if ( !frame )
			{
				LogError( "Could not find any free or unfixed pages!" );
				throw std::runtime_error( "Error: BufferManager ran out of space" );
			}
			// Try to lock, if that fails, we just start with a new page search, since
			// we do not want to wait until the lock is free again.
		} while ( !frame->TryLockWrite() );
		// We got our write lock now we do all the actual replacement work
		if ( frame->IsDirty() )
		{
			WritePage( *frame ); // Throws on fail
		}
		// Replace old id with new id and load
		uint64_t oldId = frame->mPageId;
		frame->mPageId = pageId;
		LoadPage( *frame ); // Throws on fail
		// Lock, replace, unlock
		mHashMapLock.LockWrite();
		mLoadedFrames.erase(oldId);
		mLoadedFrames.insert( std::make_pair( pageId, frame ) );
		mHashMapLock.UnlockWrite();
		// Check what kind of lock we need on our page
		if (!exclusive)
		{
			frame->Unlock();
			frame->Lock(false);
			// In the extremely extremely unlikely case that the replacement algorithm
			// chose this page to be evicted before we locked again (alg would need to do full 2 circles,
			// and by chance end up with this page and acquire the lock, all of this before we get our own lock)
			// ... 
			// well we basically have a problem, we could do a recursive FixPage call again:
			if ( frame->GetPageId() != pageId )
			{
				frame->Unlock();
				return FixPage( pageId, exclusive );
			}
		}
	}
	return *frame;
}

/// <summary>
/// Unfixes the page. // TODO More doc
/// </summary>
/// <param name="frame">The frame.</param>
/// <param name="isDirty">if set to <c>true</c> [is dirty].</param>
void BufferManager::UnfixPage( BufferFrame& frame, bool isDirty )
{
	assert( isDirty ? frame.mExclusive.load() : true ); // Iff dirty then exclusive
	// Update dirtiness and our eviction score for page replacement alg
	if ( isDirty )
	{
		frame.mEvictionScore += 2;
		frame.mDirty.store( true );
	}
	else
	{
		++frame.mEvictionScore;
	}
	frame.Unlock();
}

/// <summary>
/// Finds a replacement/free page and returns it.
/// </summary>
/// <returns>The found buffer. This returns a nullpointer in case we could not find any free buffer.</returns>
BufferFrame* BufferManager::FindReplacementPage()
{
	static std::atomic<uint64_t> sharedVectorPos(0);
	assert( mFrames.size() == mPageCount );
	assert( mPageCount != 0 );

	// Cyclic walk over at least 2*page count buffers until we find a buffer with 0 eviction score. 
	// If we could not find a single unfixed one we stop looking.
	// If we found atleast one, we continue searching.
	// If multiple pages are searched simultaneously,
	// this can also mean we skip some pages and visit some multiple times.
	bool unfixedPageExisted;
	do 
	{
		unfixedPageExisted = false;
		for ( uint32_t i = 0; i < mPageCount * 2; ++i )
		{
			uint32_t pos = (sharedVectorPos++) % mPageCount; // Atomic post increment, then mod page count
			BufferFrame& frame = mFrames[pos];
			if ( !frame.IsFixedProbably() )
			{
				if ( frame.mEvictionScore == 0 )
				{
					return &frame;
				}

				unfixedPageExisted = true;
				// We are forced to do a division, which is a non atomic operation. This is not a real problem.
				// In rare occasions, a buffer can receive some extra eviction score through this.
				frame.mEvictionScore.store( frame.mEvictionScore.load() / 2 );
			}
		}
	} while (unfixedPageExisted);
	

	// Returning a nullpointer, since we could not find any valid page
	return nullptr;
}

/// <summary>
/// Loads a page from harddrive. Throws on errors.
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
		frame.mLoaded.store( true );
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
		LogError( "Read error in segment " + segmentName + " on page " + 
				  std::to_string( ids.second ) );
		throw std::runtime_error( "Error: Reading File" );
	}

	// Set loaded bit and reset eviction score
	frame.mLoaded.store( true );
	frame.mEvictionScore.store( DB_EVICTION_COUNTER_START );

	// Cleanup
	segment.close();
}

/// <summary>
/// Writes a page to harddrive. Throws on errors.
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
		LogError( "Write error in segment " + segmentName + " on page " + 
				  std::to_string( ids.second ) );
		throw std::runtime_error( "Error: Writing File" );
	}

	// Remove dirty flag from frame
	frame.mDirty.store( false );

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

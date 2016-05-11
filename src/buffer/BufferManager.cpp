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
// - Acquire exclusive write lock on page
// - Acquire I/O lock (See Part about simultaneous loads farther down)
// - Acquire write exclusive lock on hash map
// - Remove old page entry in hash map
// - Insert new page entry into hash map
//   (We do the hash map replacement before loading/writing to disk, so other threads will see earlier that
//    a page they need is either gone/loaded by a different thread)
// - Release write exclusive lock on hash map
// - Flush page if dirty
// - Load the new page
// - Release I/O lock
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
//////////////////////////////////////////////////////////////////////////
// Simultaneous loading/writing (disk IO)
//////////////////////////////////////////////////////////////////////////
// Problem: When loading/writing pages simultaneously bad things can happen
// This can not be fixed by acquiring page locks alone: 
// Thread 1 requests page 0, does not find in map -> starts loading page 0
// Thread 2 requests page 0, does not find in map -> starts loading page 0
// Thread 1 starts loading page 0
// Thread 2 starts loading page 0
// Thread 1 finished stores in map
// Thread 2 finished but does not store in map( already there )
// Thread 2 does his thing
// Thread 2 unfixes
// Thread 2's buffer page gets evicted
// Thread 1 buffer gets removed from map without reason while he is actually still
//          working( does not do him anything bad in general, just bad page reuse )
// ->We still have a ghost page that is loaded but not in the map
// This ghost page then gets written back with wrong values once it gets evicted, not good
// Even worse scenario:
// Thread 1 requests page 0, does not find in map -> starts loading page 0
// Thread 2 requests page 0, does not find in map -> starts loading page 0
// Thread 1 finished
// Thread 1 makes changes to page
// Thread 1 unfixes page
// Thread 3 wants another page x
// Thread 3 replacement algorithm evicts page 0
// Thread 2 still loading -> Thread 3 and Thread 2 write/read from the same page on disk
// Solution 1: (old solution, but discarded once I tried to cache filestreams, because too complex)
//  Create a number of I/O mutexes, varying between page count * 2 up to 100
//  The idea behind this is to index into these with the page id and prevent
//  simultaneous reading/writing of pages.
//  We can not write more than page count pages at the same time anyways
//  so we try to minimize collisions somewhat, but don't go too crazy.
//  The upper bound just prevents a uselessly high number of mutexes,
//  we are probably not going to write/read more than 100 different pages simultaneously.
//  First we lock a mutex protecting these mutexes, then we lock the 2 mutexes corresponding
//  to the pages, then we unlock the mutex protecting the mutexes.
//  We also need to build a 2 layer mutex structure protecting our segment filestream caches.
// Solution 2: (new solution, marginally less performant, but creates a lot less headaches)
//  Disk I/O in general can not be parallelized very well anyways:
//  (http://stackoverflow.com/questions/1993699/how-to-parallelize-file-reading-and-writing)
//  So, just lock down all file access with a single big mutex and make disk I/O serial. 
//  This is preventing some small scale performance gains if we have multiple disks, 
//  or if many threads waiting in line, for the mutex even though their page has 
//  already been loaded.


/// <summary>
/// Initializes a new instance of the <see cref="BufferManager" /> class.
/// </summary>
/// <param name="pageCount">The page count.</param>
BufferManager::BufferManager( uint32_t pageCount ) : mPageCount( pageCount ),
mNotRequestedPages( 0 ), mPageMisses( 0 ), mDirtyWritebacks( 0 ), 
mPageReplacementRetries( 0 ), mSimulPageLoadTries( 0 )
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
	// Write all dirty frames back to disk
	for ( BufferFrame& f : mFrames )
	{
		// Handling fixed frames:
		// Option 1: Ignore
		// Option 2: Try to lock
		// Option 3: Assume some other system is in place that prevents this from happening
		// For now I will go with option 2 for safety
		f.Lock( true );
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
		f.Unlock();
	}
	mFrames.clear();
	// Close filestreams
	for ( std::pair<const uint64_t, std::pair<std::fstream*, uint64_t>>& mf : mFileStreams )
	{
		SDELETE( mf.second.first );
	}
	mFileStreams.clear();
	// Delete page buffer memory
	ADELETE( mBufferMemory );
}

/// <summary>
/// Fixes the page the requested page. Will load the page from disk if necessary. 
/// Do not use segment 0. Segment 0 is reserved for schema data, and DBCore has a writelock on it. 
/// Throws runtime error if no buffer space is available or if errors happen while loading/replacing pages.
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
	mHashMapLock.LockRead(); // <- Lock Read Hash Map
	auto it = mLoadedFrames.find( pageId );
	if ( it != mLoadedFrames.end() )
	{
		frame = it->second;
	}
	mHashMapLock.UnlockRead(); // <- Unlock Read Hash Map

	if (frame)
	{
		// We found the page, try to acquire our desired lock
		frame->Lock( exclusive );
		// If our page is not the correct page, we release the lock and do a recursive call to fix page
		// (Reason is explained in overview)
		return *CheckSamePage( pageId, exclusive, frame );
	}
	
	return *FixPageReplacement(pageId, exclusive);
}

/// <summary>
/// Unfixes the requested page. Will store the page to disk if it is dirty.
/// Storing will happen at some point before overwriting memory or on DB shutdown.
/// </summary>
/// <param name="frame">The frame.</param>
/// <param name="isDirty">if set to <c>true</c> [is dirty].</param>
void BufferManager::UnfixPage( BufferFrame& frame, bool isDirty )
{
	assert( frame.mExclusive.load() || frame.mSharedBy > 0 );
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
/// The page replacement part of fixing a page
/// </summary>
/// <param name="pageId">The page identifier.</param>
/// <param name="exclusive">if set to <c>true</c> [exclusive].</param>
/// <returns></returns>
BufferFrame* BufferManager::FixPageReplacement( uint64_t pageId, bool exclusive )
{
	BufferFrame* frame = nullptr;
	++mPageMisses;
	// We did not find the page, start replacement algorithm
	uint32_t pageReplaceTries = 0;
	do
	{
		frame = FindReplacementPage();
		if ( !frame )
		{
			//LogError( "Could not find any free or unfixed pages!" );
			throw std::runtime_error( "Error: BufferManager ran out of space" );
		}
		++pageReplaceTries;
		// Try to lock, if that fails, we just start with a new page search, since
		// we do not want to wait until the lock is free again.
	} while ( !frame->TryLockWrite() ); // <- Lock replacement frame
	mPageReplacementRetries += pageReplaceTries - 1;

	// Next we have to verify we are the only person reading/writing to both pages on disk
	uint64_t oldId = frame->mPageId;
	mFileIOLock.lock(); // <- Lock I/O mutex
													
	// Now check if by chance some other thread else got this lock 
	// on our page before us and the page is already in memory
	// (even if we had to wait, this does not mean it was exactly for our page)
	mHashMapLock.LockRead(); // <- Lock Read Hash Map
	auto it = mLoadedFrames.find( pageId );
	BufferFrame* altFrame = nullptr;
	if ( it != mLoadedFrames.end() )
	{
		altFrame = it->second;
	}
	mHashMapLock.UnlockRead(); // <- Unlock Read Hash Map
	if ( altFrame )
	{
		// Some other thread already loaded the page for us
		++mSimulPageLoadTries;
		frame->Unlock(); // <- Unlock replacement frame
		mFileIOLock.unlock(); // <- Unlock Page based I/O mutex
		altFrame->Lock( exclusive ); // <- Lock frame loaded by other thread
		return CheckSamePage( pageId, exclusive, altFrame );
	}

	// Replace old frame entry in hash map with new
	mHashMapLock.LockWrite(); // <- Lock Write Hash Map
	mLoadedFrames.erase( oldId );
	if ( mLoadedFrames.find( pageId ) != mLoadedFrames.end() )
	{
		// Assert against simultaneous loading
		// Search should get compiled out in Release build, since if is empty
		assert( false );
	}
	mLoadedFrames.insert( std::make_pair( pageId, frame ) );
	mHashMapLock.UnlockWrite(); // <- Unlock Write Hash Map

	// We got our 2 write locks and nobody loaded before us and we already replaced the page entry
	// in the hashmap, now we do all the actual replacement work
	if ( frame->IsDirty() )
	{
		++mDirtyWritebacks;
		WritePage( *frame ); // Throws on fail
	}

	// Replace old id with new id in frame and load
	frame->mPageId = pageId;
	LoadPage( *frame ); // Throws on fail 

	mFileIOLock.unlock(); // <- Unlock I/O mutex

	// Check what kind of lock we need on our page
	if ( !exclusive )
	{
		frame->Unlock(); // <- Swap lock replaced frame
		frame->Lock( false ); // <- Swap lock replaced frame
							  
		// In the extremely extremely unlikely case that the replacement algorithm
		// chose this page to be evicted before we locked again (alg would need to do full 2 circles,
		// and by chance end up with this page and acquire the lock, all of this before we get our own lock)
		// ... 
		// well we basically have a problem, we could do a recursive FixPage call again:
		return CheckSamePage( pageId, exclusive, frame );
	}
	return frame;
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
/// Checks the filestream cache and returns the mutex + fstream pair. Creates a new pair if necessary
/// </summary>
/// <param name="segmentId">The segment identifier.</param>
/// <returns>Pair of filestream and segment</returns>
std::pair<std::fstream*, uint64_t>& BufferManager::CheckFilestreamCache( uint64_t segmentId )
{
	// Check if segment filestream exists already
	auto fsit = mFileStreams.find( segmentId );
	
	// If the fstream does not exist, we create one and store
	if ( fsit == mFileStreams.end() )
	{
		std::fstream* f = new std::fstream();
		// Open file
		std::string segmentName = std::to_string( segmentId );
		MbOpenCreateFile( segmentName, *f );
		// Find end of file
		f->seekg( 0, std::ios::end );
		std::streampos fsize = f->tellg();
		auto p = std::make_pair( f, static_cast<uint64_t>(fsize) );
		auto inserted = mFileStreams.insert( std::make_pair( segmentId, p ) );
		if (inserted.second)
		{
			return inserted.first->second;
		}
		else
		{
			throw std::runtime_error( "Error: Filestream already cached. Segment id: " + segmentId );
		}
	}
	else
	{
		return fsit->second;
	}
}

/// <summary>
/// Opens the file in binary read write mode if the stream is not opened. Also creates the file if necessary.
/// </summary>
/// <param name="name">The name.</param>
/// <param name="stream">The stream.</param>
void BufferManager::MbOpenCreateFile( std::string& name, std::fstream& stream )
{
	if ( !stream.is_open() )
	{
		// Check existance
		if ( !FileExists( name ) )
		{
			// Quickly create an empty file
			std::ofstream newfile;
			newfile.open( name, std::ofstream::out | std::ofstream::binary );
			newfile.close();
		}
		// Open in binary mode
		stream.open( name, std::fstream::in | std::fstream::out | std::fstream::binary );
		if ( !stream.is_open() )
		{
			LogError( "Failed to open segment file " + name );
			throw std::runtime_error( "Error: Opening File" );
		}
	}
}

/// <summary>
/// Loads a page from harddrive. Throws on errors.
/// </summary>
/// <param name="pageId">The page identifier.</param>
void BufferManager::LoadPage( BufferFrame& frame )
{
	// Zero out memory
	memset( frame.mData, 0, DB_PAGE_SIZE );

	// Get a filestream
	auto ids = SplitPageId( frame.GetPageId() );
	std::pair<std::fstream*, uint64_t>& segment = CheckFilestreamCache( ids.first );
	assert( segment.first );
		
	// Read data from input file
	uint64_t pos = ids.second * DB_PAGE_SIZE;
	if (pos >= segment.second)
	{
		// Searched position is bigger than the file, just return empty
		frame.mLoaded.store( true );
		frame.mEvictionScore.store( DB_EVICTION_COUNTER_START );
		return;
	}
	segment.first->seekg( pos );
	segment.first->read( reinterpret_cast<char*>(frame.mData), DB_PAGE_SIZE );

	// Check for any errors reading (ignore eof errors, since these are normal if the page was not even created yet)
	if ( segment.first->fail() && !segment.first->eof() )
	{
		LogError( "Read error in segment " + std::to_string( ids.first ) + " on page " +
				  std::to_string( ids.second ) );
		throw std::runtime_error( "Error: Reading File" );
	}

	// Set loaded bit and reset eviction score
	frame.mLoaded.store( true );
	frame.mEvictionScore.store( DB_EVICTION_COUNTER_START );
}

/// <summary>
/// Writes a page to harddrive. Throws on errors.
/// </summary>
/// <param name="pageId">The page identifier.</param>
void BufferManager::WritePage( BufferFrame& frame )
{
	// Get a filestream
	auto ids = SplitPageId( frame.GetPageId() );
	std::pair<std::fstream*, uint64_t>& segment = CheckFilestreamCache( ids.first );
	assert( segment.first );
	
	// Find position in output file
	uint64_t pos = ids.second * DB_PAGE_SIZE;
	segment.first->seekp( pos );
	segment.first->write( reinterpret_cast<char*>(frame.mData), DB_PAGE_SIZE );

	// Check for any errors reading
	if ( segment.first->fail() )
	{
		LogError( "Write error in segment " + std::to_string( ids.first ) + " on page " +
				  std::to_string( ids.second ) );
		throw std::runtime_error( "Error: Writing File" );
	}

	// If our currently written position was bigger or equal set the new filesize
	if (pos >= segment.second)
	{
		segment.second = pos + DB_PAGE_SIZE;
	}

	// Remove dirty flag from frame
	frame.mDirty.store( false );
}

/// <summary>
/// Checks if the page is still our requested page, if it is not, we retry.
/// </summary>
/// <param name="pageId">The page identifier.</param>
/// <param name="frame">The frame.</param>
/// <returns></returns>
BufferFrame* BufferManager::CheckSamePage( uint64_t pageId, bool exclusive, BufferFrame* frame )
{
	if ( frame->GetPageId() != pageId )
	{
		++mNotRequestedPages;
		frame->Unlock();
		return &FixPage( pageId, exclusive );
	}
	return frame;
}

/// <summary>
/// Merges the page identifier.
/// </summary>
/// <param name="segmentId">The 16bit segment identifier.</param>
/// <param name="pageInSegment">The 48bit page in segment.</param>
/// <returns></returns>
uint64_t BufferManager::MergePageId( uint64_t segmentId, uint64_t pageInSegment )
{
	segmentId = (segmentId & 0xFFFFul) << 48;
	pageInSegment = pageInSegment & 0x0000FFFFFFFFFFFFul;
	return segmentId | pageInSegment;
}

/// <summary>
/// Splits the page identifier into SegmentId and page in segment
/// </summary>
/// <param name="pageId">The page identifier.</param>
/// <returns>16 bit segmentid, 48 bit page in segment</returns>
std::pair<uint64_t, uint64_t> BufferManager::SplitPageId( uint64_t pageId )
{
	uint64_t segmentId = (pageId & 0xFFFF000000000000ul) >> 48;
	assert( segmentId <= 0xFFFFul );
	uint64_t nPageId = pageId & 0x0000FFFFFFFFFFFFul;
	assert( segmentId <= 0xFFFFFFFFFFFFul );
	return std::make_pair( segmentId, nPageId );
}
